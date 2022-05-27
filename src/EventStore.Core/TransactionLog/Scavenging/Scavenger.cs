﻿using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Log;
using EventStore.Core.Data;
using EventStore.Core.TransactionLog.Chunks;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class Scavenger {
		protected static readonly ILogger Log = LogManager.GetLoggerFor<Scavenger>();
	}

	public class Scavenger<TStreamId> : Scavenger, IScavenger {
		private readonly IScavengeState<TStreamId> _state;
		private readonly IAccumulator<TStreamId> _accumulator;
		private readonly ICalculator<TStreamId> _calculator;
		private readonly IChunkExecutor<TStreamId> _chunkExecutor;
		private readonly IChunkMerger _chunkMerger;
		private readonly IIndexExecutor<TStreamId> _indexExecutor;
		private readonly ICleaner _cleaner;
		private readonly IScavengePointSource _scavengePointSource;
		private readonly ITFChunkScavengerLog _scavengerLogger;

		public Scavenger(
			IScavengeState<TStreamId> state,
			IAccumulator<TStreamId> accumulator,
			ICalculator<TStreamId> calculator,
			IChunkExecutor<TStreamId> chunkExecutor,
			IChunkMerger chunkMerger,
			IIndexExecutor<TStreamId> indexExecutor,
			ICleaner cleaner,
			IScavengePointSource scavengePointSource,
			ITFChunkScavengerLog scavengerLogger) {

			_state = state;
			_accumulator = accumulator;
			_calculator = calculator;
			_chunkExecutor = chunkExecutor;
			_chunkMerger = chunkMerger;
			_indexExecutor = indexExecutor;
			_cleaner = cleaner;
			_scavengePointSource = scavengePointSource;
			_scavengerLogger = scavengerLogger;
		}

		public string ScavengeId => _scavengerLogger.ScavengeId;

		public async Task ScavengeAsync(CancellationToken cancellationToken) {

			Log.Trace("SCAVENGING: started scavenging DB.");
			LogCollisions();

			var stopwatch = Stopwatch.StartNew();

			var result = ScavengeResult.Success;
			string error = null;
			try {
				_scavengerLogger.ScavengeStarted();

				await RunInternal(_scavengerLogger, stopwatch, cancellationToken);

				Log.Trace(
					"SCAVENGING: total time taken: {elapsed}, total space saved: {spaceSaved}.",
					stopwatch.Elapsed, _scavengerLogger.SpaceSaved);

			} catch (OperationCanceledException) {
				Log.Info("SCAVENGING: Scavenge cancelled.");
				result = ScavengeResult.Stopped;
			} catch (Exception exc) {
				result = ScavengeResult.Failed;
				Log.ErrorException(exc, "SCAVENGING: error while scavenging DB.");
				error = string.Format("Error while scavenging DB: {0}.", exc.Message);
			} finally {
				LogCollisions();
				try {
					_scavengerLogger.ScavengeCompleted(result, error, stopwatch.Elapsed);
				} catch (Exception ex) {
					Log.ErrorException(
						ex,
						"Error whilst recording scavenge completed. " +
						"Scavenge result: {result}, Elapsed: {elapsed}, Original error: {e}",
						result, stopwatch.Elapsed, error);
				}
			}
		}

		private void LogCollisions() {
			var collisions = _state.AllCollisions().ToArray();
			Log.Trace("{count} KNOWN COLLISIONS", collisions.Length);

			foreach (var collision in collisions) {
				Log.Trace("KNOWN COLLISION: \"{collision}\"", collision);
			}
		}

		private async Task RunInternal(
			ITFChunkScavengerLog scavengerLogger,
			Stopwatch stopwatch,
			CancellationToken cancellationToken) {

			//qq consider exceptions (cancelled, and others)

			// each component can be started with either
			//  (i) a checkpoint that it wrote previously (it will continue from there)
			//  (ii) fresh from a given scavengepoint
			//
			// so if we have a checkpoint from a component, start the scavenge by passing the checkpoint
			// into that component and then starting each subsequent component fresh for that
			// scavengepoint.
			//
			// otherwise, start the whole scavenge fresh from whichever scavengepoint is applicable.
			if (!_state.TryGetCheckpoint(out var checkpoint)) {
				// there is no checkpoint, so this is the first scavenge of this scavenge state
				// (not necessarily the first scavenge of this database, old scavenged may have been run
				// or new scavenges run and the scavenge state deleted)
				await StartNewAsync(
					prevScavengePoint: null,
					scavengerLogger,
					stopwatch,
					cancellationToken);

			} else if (checkpoint is ScavengeCheckpoint.Done done) {
				// start of a subsequent scavenge.
				await StartNewAsync(
					prevScavengePoint: done.ScavengePoint,
					scavengerLogger,
					stopwatch,
					cancellationToken);

				// the other cases are continuing an incomplete scavenge
			} else if (checkpoint is ScavengeCheckpoint.Accumulating accumulating) {
				Time(stopwatch, "Accumulation", () =>
					_accumulator.Accumulate(accumulating, _state, cancellationToken));
				AfterAccumulation(
					accumulating.ScavengePoint, scavengerLogger, stopwatch, cancellationToken);

			} else if (checkpoint is ScavengeCheckpoint.Calculating<TStreamId> calculating) {
				Time(stopwatch, "Calculation", () =>
					_calculator.Calculate(calculating, _state, cancellationToken));
				AfterCalculation(
					calculating.ScavengePoint, scavengerLogger, stopwatch, cancellationToken);

			} else if (checkpoint is ScavengeCheckpoint.ExecutingChunks executingChunks) {
				Time(stopwatch, "Chunk execution", () =>
					_chunkExecutor.Execute(executingChunks, _state, scavengerLogger, cancellationToken));
				AfterChunkExecution(
					executingChunks.ScavengePoint, scavengerLogger, stopwatch, cancellationToken);

			} else if (checkpoint is ScavengeCheckpoint.MergingChunks mergingChunks) {
				Time(stopwatch, "Chunk merging", () =>
					_chunkMerger.MergeChunks(mergingChunks, _state, scavengerLogger, cancellationToken));
				AfterChunkMerging(
					mergingChunks.ScavengePoint, scavengerLogger, stopwatch, cancellationToken);

			} else if (checkpoint is ScavengeCheckpoint.ExecutingIndex executingIndex) {
				Time(stopwatch, "Index execution", () =>
					_indexExecutor.Execute(executingIndex, _state, scavengerLogger, cancellationToken));
				AfterIndexExecution(
					executingIndex.ScavengePoint, stopwatch, cancellationToken);

			} else if (checkpoint is ScavengeCheckpoint.Cleaning cleaning) {
				Time(stopwatch, "Cleaning", () =>
					_cleaner.Clean(cleaning, _state, cancellationToken));
				AfterCleaning(cleaning.ScavengePoint);

			} else {
				throw new Exception($"Unexpected checkpoint: {checkpoint}");
			}
		}

		private void Time(Stopwatch stopwatch, string name, Action f) {
			var start = stopwatch.Elapsed;
			f();
			var elapsed = stopwatch.Elapsed - start;
			Log.Trace("{name} took {elapsed}", name, elapsed);
		}

		private async Task StartNewAsync(
			ScavengePoint prevScavengePoint,
			ITFChunkScavengerLog scavengerLogger,
			Stopwatch stopwatch,
			CancellationToken cancellationToken) {

			// prevScavengePoint is the previous one that was completed
			// latestScavengePoint is the latest one in the database
			// nextScavengePoint is the one we are about to scavenge up to

			// threshold < 0: execute all chunks, even those with no weight
			// threshold = 0: execute all chunks with weight greater than 0
			// threshold > 0: execute all chunks above a certain weight
			var threshold = 0;
			ScavengePoint nextScavengePoint;
			var latestScavengePoint = await _scavengePointSource.GetLatestScavengePointOrDefaultAsync();
			if (latestScavengePoint == null) {
				Log.Trace("SCAVENGING: creating the first scavenge point.");
				// no latest scavenge point, create the first one
				nextScavengePoint = await _scavengePointSource
					.AddScavengePointAsync(ExpectedVersion.NoStream, threshold: threshold);
			} else {
				// got the latest scavenge point
				if (prevScavengePoint == null ||
					prevScavengePoint.EventNumber < latestScavengePoint.EventNumber) {
					// the latest scavengepoint is suitable
					Log.Trace(
						"SCAVENGING: using existing scavenge point {scavengePointNumber}",
						latestScavengePoint.EventNumber);
					nextScavengePoint = latestScavengePoint;
				} else {
					// the latest scavengepoint is the prev scavenge point, so create a new one
					var expectedVersion = prevScavengePoint.EventNumber;
					//qq test that if this fails the scavenge terminates with an error
					Log.Trace(
						"SCAVENGING: creating the next scavenge point: {scavengePointNumber}",
						expectedVersion + 1);

					nextScavengePoint = await _scavengePointSource
						.AddScavengePointAsync(expectedVersion, threshold: threshold);
				}
			}

			// we now have a nextScavengePoint.
			Time(stopwatch, "Accumulation", () =>
				_accumulator.Accumulate(prevScavengePoint, nextScavengePoint, _state, cancellationToken));
			AfterAccumulation(nextScavengePoint, scavengerLogger, stopwatch, cancellationToken);
		}

		private void AfterAccumulation(
			ScavengePoint scavengepoint,
			ITFChunkScavengerLog scavengerLogger,
			Stopwatch stopwatch,
			CancellationToken cancellationToken) {

			Time(stopwatch, "Calculation", () =>
				_calculator.Calculate(scavengepoint, _state, cancellationToken));
			AfterCalculation(scavengepoint, scavengerLogger, stopwatch, cancellationToken);
		}

		private void AfterCalculation(
			ScavengePoint scavengePoint,
			ITFChunkScavengerLog scavengerLogger,
			Stopwatch stopwatch,
			CancellationToken cancellationToken) {

			Time(stopwatch, "Chunk execution", () =>
				_chunkExecutor.Execute(scavengePoint, _state, scavengerLogger, cancellationToken));
			AfterChunkExecution(scavengePoint, scavengerLogger, stopwatch, cancellationToken);
		}

		private void AfterChunkExecution(
			ScavengePoint scavengePoint,
			ITFChunkScavengerLog scavengerLogger,
			Stopwatch stopwatch,
			CancellationToken cancellationToken) {

			Time(stopwatch, "Chunk merging", () =>
				_chunkMerger.MergeChunks(scavengePoint, _state, scavengerLogger, cancellationToken));
			AfterChunkMerging(scavengePoint, scavengerLogger, stopwatch, cancellationToken);
		}

		private void AfterChunkMerging(
			ScavengePoint scavengePoint,
			ITFChunkScavengerLog scavengerLogger,
			Stopwatch stopwatch,
			CancellationToken cancellationToken) {

			Time(stopwatch, "Index execution", () =>
				_indexExecutor.Execute(scavengePoint, _state, scavengerLogger, cancellationToken));
			AfterIndexExecution(scavengePoint, stopwatch, cancellationToken);
		}

		private void AfterIndexExecution(
			ScavengePoint scavengePoint,
			Stopwatch stopwatch,
			CancellationToken cancellationToken) {

			Time(stopwatch, "Cleaning", () =>
				_cleaner.Clean(scavengePoint, _state, cancellationToken));
			AfterCleaning(scavengePoint);
		}

		private void AfterCleaning(ScavengePoint scavengePoint) {
			_state.SetCheckpoint(new ScavengeCheckpoint.Done(scavengePoint));
		}
	}
}
