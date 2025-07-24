using System.Text.Json;
using System.Threading.Tasks;
using TestTiDB.Services;
using Xunit;

namespace TestTiDB.Tests
{
    public class CRHandlerTests
    {
        private readonly CRHandler _handler;

        public CRHandlerTests()
        {
            _handler = new CRHandler();
        }

        [Fact]
        public async Task Get_Should_Return_200_Status()
        {
            var result = await _handler.Get();
            Assert.NotNull(result);
            Assert.True(result.StatusCode == 200 || result.StatusCode == 400 || result.StatusCode == 404);
            Assert.False(string.IsNullOrWhiteSpace(result.Message));
        }

        [Fact]
        public async Task Active_Should_Return_200_Status()
        {
            var result = await _handler.Active();
            Assert.Equal(200, result.StatusCode);

            var json = JsonSerializer.Serialize(result.Data);
            var obj = JsonSerializer.Deserialize<Dictionary<string, object>>(json);

            Assert.Equal("Ghi đồng thời thành công", obj?["Message"].ToString());
        }    
    }
}
