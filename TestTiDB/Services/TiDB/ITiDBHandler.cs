using Microsoft.AspNetCore.Mvc;

namespace TestTiDB.Services
{
    public interface ITiDBHandler
    {
        Task<HandlerResult> GetTidb();
        Task<HandlerResult> GetTidbOptimized();
        Task<HandlerResult> GetTidbps();
    }
}
