using System.Threading.Tasks;
using Xunit;
using TestTiDB.Services;

namespace TestTiDB.Tests
{
    public class SQLServerHandlerTests
    {
        private readonly SQLServerhandler _handler;

        public SQLServerHandlerTests()
        {
            _handler = new SQLServerhandler(); 
        }

        [Fact]
        public async Task GetSqlServer_Should_Return_Valid_Result()
        {
            var result = await _handler.GetSqlServer();

            Assert.NotNull(result);
            Assert.True(result.StatusCode == 200 || result.StatusCode == 400 || result.StatusCode == 404);
            Assert.False(string.IsNullOrWhiteSpace(result.Message));
        }

        [Fact]
        public async Task GetSqlServerWithSP_Should_Return_Valid_Result()
        {
            var result = await _handler.GetSqlServerWithSP();

            Assert.NotNull(result);
            Assert.True(result.StatusCode == 200 || result.StatusCode == 400 || result.StatusCode == 404);
            Assert.False(string.IsNullOrWhiteSpace(result.Message));
        }
    }
}
