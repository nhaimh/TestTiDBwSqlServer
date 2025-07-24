using Microsoft.AspNetCore.Mvc;

namespace TestTiDB.Services
{
    public interface ICRHandler
    {
        Task<HandlerResult> Get();
        Task<HandlerResult> Active();
    }
}
