﻿using System;
using System.Collections.Generic;
using System.Threading;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq add logging to this and the other stages
	public class ChunkExecutor<TStreamId, TChunk> : IChunkExecutor<TStreamId> {

		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;
		private readonly IChunkManagerForChunkExecutor<TStreamId, TChunk> _chunkManager;
		private readonly long _chunkSize;
		private readonly bool _unsafeIgnoreHardDeletes;
		private readonly int _cancellationCheckPeriod;

		public ChunkExecutor(
			IMetastreamLookup<TStreamId> metastreamLookup,
			IChunkManagerForChunkExecutor<TStreamId, TChunk> chunkManager,
			long chunkSize,
			bool unsafeIgnoreHardDeletes,
			int cancellationCheckPeriod) {

			_metastreamLookup = metastreamLookup;
			_chunkManager = chunkManager;
			_chunkSize = chunkSize;
			_unsafeIgnoreHardDeletes = unsafeIgnoreHardDeletes;
			_cancellationCheckPeriod = cancellationCheckPeriod;
		}

		public void Execute(
			ScavengePoint scavengePoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			CancellationToken cancellationToken) {

			var checkpoint = new ScavengeCheckpoint.ExecutingChunks(
				scavengePoint: scavengePoint,
				doneLogicalChunkNumber: default);
			state.SetCheckpoint(checkpoint);
			Execute(checkpoint, state, cancellationToken);
		}

		public void Execute(
			ScavengeCheckpoint.ExecutingChunks checkpoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			CancellationToken cancellationToken) {

			//qq would we want to run in parallel? (be careful with scavenge state interactions
			// in that case, especially writes and storing checkpoints)
			//qq order by the weight? maybe just iterate backwards.

			//qq there is no point scavenging beyond the scavenge point
			//qqqq is +1 ok range wise? same for accumulator
			var startFromChunk = checkpoint?.DoneLogicalChunkNumber + 1 ?? 0; //qq necessarily zero?
			var scavengePoint = checkpoint.ScavengePoint;

			foreach (var physicalChunk in GetAllPhysicalChunks(startFromChunk, scavengePoint)) {
				var transaction = state.BeginTransaction();
				try {
					var physicalWeight = state.SumChunkWeights(
						physicalChunk.ChunkStartNumber,
						physicalChunk.ChunkEndNumber);

					if (physicalWeight > scavengePoint.Threshold || _unsafeIgnoreHardDeletes) {
						ExecutePhysicalChunk(scavengePoint, state, physicalChunk, cancellationToken);

						state.ResetChunkWeights(
							physicalChunk.ChunkStartNumber,
							physicalChunk.ChunkEndNumber);
					}

					cancellationToken.ThrowIfCancellationRequested();

					transaction.Commit(
						new ScavengeCheckpoint.ExecutingChunks(
							scavengePoint,
							physicalChunk.ChunkEndNumber));
				} catch {
					//qq here might be sensible place, the old scavenge handles various exceptions
					// FileBeingDeletedException, OperationCanceledException, Exception
					// with logging and without stopping the scavenge i think. consider what we want to do
					// but be careful that if we allow the scavenge to continue without having executed
					// this chunk, we can't assume later that the scavenge was really completed, which
					// has implications for the cleaning phase, especially with _unsafeIgnoreHardDeletes
					transaction.Rollback();
					throw;
				}
			}
		}

		//qqqq the scavenge point can, itself, be in a merged chunk.
		// we can decide here whether to
		// 1. scavenge the merge chunk with it in, and respect the scavenge
		//    point as we go. currently this respects the discard points, which is enough to also
		//    respect the scavenge point, but using the scavenge point directly would be more efficient
		//    if there is any chance a record will be the other side of it. OR
		// 2. not to scavenge the chunk with the discard point in it, so every chunk we execute only has
		//    records that are before the scavenge point.
		//    see how awkward the former is when we bring in the older logic. remember not scavenging
		//    things has implications for gdpr.
		private IEnumerable<IChunkReaderForExecutor<TStreamId>> GetAllPhysicalChunks(
			int startFromChunk,
			ScavengePoint scavengePoint) {

			var scavengePos = _chunkSize * startFromChunk;
			var upTo = scavengePoint.Position;
			while (scavengePos < upTo) {
				var physicalChunk = _chunkManager.GetChunkReaderFor(scavengePos);

				if (!physicalChunk.IsReadOnly)
					yield break;

				yield return physicalChunk;

				scavengePos = physicalChunk.ChunkEndPosition;
			}
		}

		private void ExecutePhysicalChunk(
			ScavengePoint scavengePoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			IChunkReaderForExecutor<TStreamId> chunk,
			CancellationToken cancellationToken) {

			//qq the other reason we might want to not scanvenge this chunk is if the posmap would make
			// it bigger
			// than the original... limited concern because of the threshold above BUT we could address
			// by using a padding/scavengedevent system event to prevent having to write a posmap
			// this is the kind of decision we can make in here, local to the chunk.
			// knowing the numrecordstodiscard could be useful here, if we are just discarding a small
			// number then we'd probably pad them with 'gone' events instead of adding a posmap.

			//qq in ExecuteChunk could also be a reasonable place to do a best effort at removing commit
			// records if all the prepares for the commit are in this chunk (typically the case) and they
			// are all scavenged, then we can remove the commit as well i think. this is probably what
			// the old scavenge does. check

			//qq old scavenge says 'never delete the very first prepare in a transaction'
			// hopefully we can account for that here? although maybe it means our count of
			// records to scavenge that was calculated index only might end up being approximate.

			//qqqq TRANSACTIONS
			//qq add tests that makes sure its ok when we have uncommitted transactions that "collide"
			//
			// ChunkExecutor:
			// - we can only scavenge a record if we know what event number it is, for which we need the commit
			//   record. so perhaps we only scavenge the events in a transaction (and the commit record)
			//   if the transaction was committed in the same chunk that it was started. this is pretty much
			//   what the old scavenge does too.
			//
			//  TRANSACTIONS IN OLD SCAVENGE
			//   - looks like uncommitted prepares are generally kept, maybe unless the stream is hard deleted
			//   - looks like even committed prepares are only removed if the commit is in the same chunk
			// - uncommitted transactions are not present in the index, neither are commit records
			//
			//qq what if the lastevent in a stream is in a transaction?
			//     we need to make sure we keep it, even though it doesn't have a expectedversion
			//     so we can do just like old scavenge. if we cant establish the expectedversion then just keep it
			//     if we can establish the expected version then we can compare it to the discard point.
			//qq can we scavenge stuff better if the stream is known to be hard deleted - yes, we can get rid of
			// everything except the begin records (just like old scavenge)
			//   note in this case the lasteventnumber is the tombstone, which is not in a transaction.


			// 1. open the chunk, probably with the bulk reader
			var newChunk = _chunkManager.CreateChunkWriter(
				chunk.ChunkStartNumber,
				chunk.ChunkEndNumber);

			var cancellationCheckCounter = 0;
			foreach (var record in chunk.ReadRecords()) {
				var discard = ShouldDiscard(
					state,
					scavengePoint,
					record);

				if (discard) {
					//qq discard record
				} else {
					//qq keep record
					newChunk.WriteRecord(record); //qq or similar
					//qq do we need to upgrade it?
					//qq will using the bulk reader be awkward considering the record format
					// size changes that have occurred over the years
					// if so consider using the regular reader.
					// what does the old scavenge use
					// consider transactions
				}

				if (++cancellationCheckCounter == _cancellationCheckPeriod) {
					cancellationCheckCounter = 0;
					cancellationToken.ThrowIfCancellationRequested();
				}
			}

			// 2. read through it, keeping and discarding as necessary. probably no additional lookups at
			// this point
			// 3. write the posmap
			// 4. finalise the chunk
			// 5. swap it in to the chunkmanager
			_chunkManager.SwitchChunk(
				newChunk.WrittenChunk,
				verifyHash: default, //qq
				removeChunksWithGreaterNumbers: default, //qq
				out var newFileName);
			//qq what is the new file name of an inmemory chunk :/
			//qq log
		}

		private bool ShouldDiscard(
			IScavengeStateForChunkExecutor<TStreamId> state,
			ScavengePoint scavengePoint,
			RecordForScavenge<TStreamId> record) {

			if (!record.IsScavengable)
				return false;

			if (record.EventNumber < 0) {
				// we could discard from transactions sometimes, either by accumulating a state for them
				// or doing a similar trick as old scavenge and limiting it to transactions that were
				// stated and commited in the same chunk. however for now this isn't considered so
				// important because someone with transactions to scavenge has probably scavenged them
				// already with old scavenge. could be added later
				return false;
			}

			//qq consider how/where to cache the this stuff per stream for quick lookups
			var details = GetStreamExecutionDetails(
				state,
				record.StreamId);

			if (details.IsTombstoned) {
				if (_unsafeIgnoreHardDeletes) {
					// remove _everything_ for metadata and original streams
					return true;
				}

				if (_metastreamLookup.IsMetaStream(record.StreamId)) {
					// when the original stream is tombstoned we can discard the _whole_ metadata stream
					return true;
				}

				// otherwise obey the discard points below.
			}

			// if definitePoint says discard then discard.
			if (details.DiscardPoint.ShouldDiscard(record.EventNumber)) {
				return true;
			}

			// if maybeDiscardPoint says discard then maybe we can discard - depends on maxage
			if (!details.MaybeDiscardPoint.ShouldDiscard(record.EventNumber)) {
				// both discard points said do not discard, so dont.
				return false;
			}

			// discard said no, but maybe discard said yes
			if (!details.MaxAge.HasValue) {
				return false;
			}

			return record.TimeStamp < scavengePoint.EffectiveNow - details.MaxAge;
		}

		private ChunkExecutionInfo GetStreamExecutionDetails(
			IScavengeStateForChunkExecutor<TStreamId> state,
			TStreamId streamId) {

			if (_metastreamLookup.IsMetaStream(streamId)) {
				if (!state.TryGetMetastreamData(streamId, out var metastreamData)) {
					metastreamData = MetastreamData.Empty;
				}

				return new ChunkExecutionInfo(
					isTombstoned: metastreamData.IsTombstoned,
					discardPoint: metastreamData.DiscardPoint,
					maybeDiscardPoint: DiscardPoint.KeepAll,
					maxAge: null);
			} else {
				// original stream
				if (state.TryGetChunkExecutionInfo(streamId, out var details)) {
					return details;
				} else {
					return new ChunkExecutionInfo(
						isTombstoned: false,
						discardPoint: DiscardPoint.KeepAll,
						maybeDiscardPoint: DiscardPoint.KeepAll,
						maxAge: null);
				}
			}
		}
	}
}
