﻿using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class UnsafeIgnoreHardDeletesTests {
		//qq UNSAFE IGNORE HARD DELETES
		// - it would mean the scavenge would have to make sure it removed all the records from all the
		//   chunks and the index (i.e. threshold 0, or 1, //qq and never keep unscavenged if smaller
		// - also it goes without saying that bad things will happen if they mix chunks in from
		//   a node that has not had the scavenge that removed the events

		[Fact]
		public async Task simple_tombstone() {
			var t = 0;
			var (state, db) = await new Scenario()
				.WithUnsafeIgnoreHardDeletes()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", metadata: MaxCount1),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Delete(t++, "ab-1"))
					.Chunk(ScavengePointRec(t++, threshold: 1000)))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(),
					x.Recs[1],
				});

			Assert.False(state.TryGetOriginalStreamData("ab-1", out _));
			Assert.False(state.TryGetMetastreamData("$$ab-1", out _));
		}

		[Fact]
		public async Task tombstone_then_normal_scavenge_then_unsafeharddeletes() {
			// after the normal scavenge of a tombstoned stream we still need to be
			// able to remove the tombstone with a unsafeharddeletes scavenge

			// the normal scavenge with the tombstone
			var t = 0;
			var scenario = new Scenario();
			var (state, db) = await scenario
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", metadata: MaxCount1),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Delete(t++, "ab-1"))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
					// only the tombstone is kept
					x.Recs[0].KeepIndexes(3),
					x.Recs[1],
				});

			// the second scavenge with unsafeharddeletes
			(state, db) = await new Scenario()
				//qq.WithTracerFrom(scenario)
				.WithDb(db)
				.WithState(x => x.ExistingState(state))
				.WithUnsafeIgnoreHardDeletes()
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(), // tombstone has gone from the chunk
						new LogRecord[] { null, null }, // two scavenge points
					},
					x => new[] {
						x.Recs[0].KeepIndexes(), // tombstone has gone from the index
						new LogRecord[] { null }, // (didn't index second scavengepoint)
					});

			Assert.False(state.TryGetOriginalStreamData("ab-1", out _));
			Assert.False(state.TryGetMetastreamData("$$ab-1", out _));
		}

		[Fact]
		public async Task normal_scavenge_then_tombstone_then_unsafeharddeletes() {
			// after the normal scavenge runs and clears the metastreamdatas,
			// we need to be able to tombstone a stream and still be able to remove
			// the leftover metadata record.

			// the normal scavenge with the tombstone
			var t = 0;
			var scenario = new Scenario();
			var (state, db) = await scenario
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", metadata: MaxCount1),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk(ScavengePointRec(t++)) // SP-0
					.Chunk(Rec.Delete(t++, "ab-1"))
					.Chunk(ScavengePointRec(t++))) // SP-1
				.MutateState(x => {
					// make it start with SP-0
					x.SetCheckpoint(new ScavengeCheckpoint.Accumulating(
						ScavengePoint(
							chunk: 1,
							eventNumber: 0),
						doneLogicalChunkNumber: null));
				})
				.AssertTrace(
					Tracer.Line("Accumulating from checkpoint: Accumulating SP-0 done None"),
					Tracer.AnythingElse)
				// result of scavenging SP-0
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(0, 2),
					x.Recs[1],
					x.Recs[2],
					x.Recs[3],
				});

			// the second scavenge with unsafeharddeletes
			(state, db) = await new Scenario()
				.WithTracerFrom(scenario)
				.WithDb(db)
				.WithState(x => x.ExistingState(state))
				.WithUnsafeIgnoreHardDeletes()
				.AssertTrace(
					Tracer.Line("Accumulating from SP-0 to SP-1"),
					Tracer.AnythingElse)
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(), // metadata record has gone
					x.Recs[1],
					x.Recs[2].KeepIndexes(), // tombstone has gone
					x.Recs[3],
				});

			Assert.False(state.TryGetOriginalStreamData("ab-1", out _));
			Assert.False(state.TryGetMetastreamData("$$ab-1", out _));
		}
	}
}
