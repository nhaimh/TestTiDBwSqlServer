using System.Threading.Tasks;
using Xunit;
using TestTiDB.Services;

namespace TestTiDB.Tests
{
    public class TiDBHandlerTests
    {
        private readonly TiDBHandler _handler;

        public TiDBHandlerTests()
        {
            _handler = new TiDBHandler(); 
        }

        [Fact]
        public async Task GetTidb_Should_Return_Valid_Result()
        {
            var result = await _handler.GetTidb();

            Assert.NotNull(result);
            Assert.True(result.StatusCode == 200 || result.StatusCode == 400 || result.StatusCode == 404);
            Assert.False(string.IsNullOrWhiteSpace(result.Message));
        }

        [Fact]
        public async Task GetTidbOptimized_Should_Return_Valid_Result()
        {
            var result = await _handler.GetTidbOptimized();

            Assert.NotNull(result);
            Assert.True(result.StatusCode == 200 || result.StatusCode == 400 || result.StatusCode == 404);
            Assert.False(string.IsNullOrWhiteSpace(result.Message));
        }

        [Fact]
        public async Task GetTidbps_Should_Return_Valid_Result()
        {
            var result = await _handler.GetTidbps();

            Assert.NotNull(result);
            Assert.True(result.StatusCode == 200 || result.StatusCode == 400 || result.StatusCode == 404);
            Assert.False(string.IsNullOrWhiteSpace(result.Message));
        }
    }
}
