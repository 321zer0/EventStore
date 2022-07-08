using System;
using System.Text;
using EventStore.Core.LogAbstraction;
using LogV3StreamId = System.UInt32;

namespace EventStore.Core.LogV2 {
	public class LogV2StreamIdConverter : IStreamIdConverter<string> {
		public string ToStreamId(LogV3StreamId x) {
			throw new System.NotImplementedException();
		}
	}

	public class LogV2RawStreamIdConverter : IRawStreamIdConverter<string> {
		public string ToStreamId(ReadOnlySpan<byte> bytes) {
			unsafe {
				fixed (byte* b = bytes) {
					return Encoding.UTF8.GetString(b, bytes.Length);
				}
			}
		}

		public string ToStreamId(ReadOnlyMemory<byte> bytes) =>
			ToStreamId(bytes.Span);
	}
}
