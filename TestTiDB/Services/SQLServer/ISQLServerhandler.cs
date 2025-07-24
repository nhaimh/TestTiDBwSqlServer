using Microsoft.AspNetCore.Mvc;

namespace TestTiDB.Services
{
    public interface ISQLServerhandler
    {
        Task<HandlerResult> GetSqlServer();
        Task<HandlerResult> GetSqlServerWithSP();
    }
}
