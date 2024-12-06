using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Tooling.Connector;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace Durable2
{
    public static class Function1
    {
        private static string responseContent;
        static CrmServiceClient serviceClient = null;
        static CosmosClient cosmosClient = new CosmosClient("AccountEndpoint=https://ea-cosmos-dev.documents.azure.com:443/;AccountKey=<your account key>");
        private static string clientId = "e6130d5d-a165-44a9-8964-6f11ed59549f";
        private static string clientSecret = "<your client secret>";
        private static string authority = "https://login.microsoftonline.com/efe64105-431e-4df2-abda-608a635e081f";
        private static string dataverseUrl = "https://org798d1247.crm.dynamics.com";
        //static ILogger logger = null;

        [Function(nameof(Function1))]
        public static async Task<OrchastratorResponse> RunOrchestrator(
    [OrchestrationTrigger] TaskOrchestrationContext context)
        {
            var rcbc = context.GetInput<Tuple<string, string>>();
            ILogger logger = null;
            var finalResponse = new OrchastratorResponse();
            int failCounter = 0; int successCounter = 0;
            if (rcbc == null) { throw new ArgumentException("rcbc is null bc"); }
            var recCount = int.Parse(rcbc.Item1);
            var batchCount = int.Parse(rcbc.Item2);

            List<SalesRecord> finalSales = new List<SalesRecord>();
            List<CrmResponse> finalCrmResponse = new List<CrmResponse>();
            logger = context.CreateReplaySafeLogger(nameof(Function1));

            logger.LogInformation($"value of rc is {recCount}");
            logger.LogInformation($"value of bc is {batchCount}");
            List<Task<CosmosResponse>> cosmosTasks = new List<Task<CosmosResponse>>();

            finalResponse.OrchastrationId = Guid.Parse(context.InstanceId);

            if (recCount <= batchCount)
            {
                cosmosTasks.Add(context.CallActivityAsync<CosmosResponse>(nameof(GetCosmoData), $"0,{batchCount}"));
            }

            else
            {
                int i = 0;
                //cosmosTasks.Add(context.CallActivityAsync<List<SalesRecord>>(nameof(GetCosmoData), "0,900"));
                while (i < recCount)
                {
                    //if (!context.IsReplaying)
                    logger.LogInformation($"i = {i} and batchCount={batchCount}");
                    var packet = Tuple.Create(failCounter, successCounter, batchCount, $"{i},{batchCount}");
                    //var packet = (failCounter, successCounter, batchCount, );
                    cosmosTasks.Add(context.CallActivityAsync<CosmosResponse>(nameof(GetCosmoData), packet));
                    //cosmosTasks.Add(context.CallActivityAsync<List<SalesRecord>>(nameof(GetCosmoData), $"{i},{batchCount}"));
                    i += batchCount;
                }
            }
            try
            {
                await Task.WhenAll(cosmosTasks);
                //await context.CallActivityAsync(nameof(WaitActivity), cosmosTasks);
            }
            catch (AggregateException ax)
            {
                if (logger != null)
                    logger.LogError(ax.StackTrace);
            }


            List<Task<List<CrmResponse>>> crmTasks = new List<Task<List<CrmResponse>>>();

            finalResponse.CosmosReadSuccessCount = cosmosTasks.Sum(t => t.Result.SuccessCount);
            finalResponse.CosmosReadFailureCount = cosmosTasks.Sum(t => t.Result.FailCount);
            logger.LogInformation($" FINAL for instance {context.InstanceId},Success= {finalResponse.CosmosReadSuccessCount} and Failed = {finalResponse.CosmosReadFailureCount}");

            cosmosTasks.ForEach(action: t =>
            {
                if (t.Result.TotalRecords.Count > 0)
                    crmTasks.Add(context.CallActivityAsync<List<CrmResponse>>(nameof(InsertCRM), t.Result.TotalRecords));
                else
                    logger.LogInformation("skipping CRM insert because of 0 records");
            });

            //if (!context.IsReplaying)
            //  logger.LogInformation($"Sending {crmTasks.Sum(t => t.Result.Count)} total cosmos results for CRM insert");
            //successCounter = failCounter = 0;
            //await context.CallActivityAsync(nameof(WaitActivity));
            await Task.WhenAll(crmTasks);

            foreach (var crmTask in crmTasks)
            {
                finalResponse.CRMWriteSuccessCount += crmTask.Result.Count(item => !item.IsFaulted.Value);
                finalResponse.CRMWriteFailureCount += batchCount * crmTask.Result.Count(item => item.IsFaulted.Value);
                ////finalResponse.Exceptions = new List<string>(crmTask.Result.Select(item => item.ExceptionText));
            }


            return finalResponse;
        }

        private static async Task<CreateMultipleResponseWebApi> ExecuteMultipleDurableSales(string jsonPayload)
        {
            CreateMultipleResponseWebApi apiResponse = null;

            using (var httpClient = new HttpClient())
            {
                var request = new HttpRequestMessage(HttpMethod.Post, $"{dataverseUrl}/api/data/v9.2/adf_durablesaleses/Microsoft.Dynamics.CRM.CreateMultiple");

                request.Headers.Add("OData-MaxVersion", "4.0");
                request.Headers.Add("OData-Version", "4.0");
                request.Headers.IfNoneMatch.Add(new EntityTagHeaderValue("\"null\""));
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await GetAccessToken());
                request.Content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
                var response = await httpClient.SendAsync(request);
                if (response.IsSuccessStatusCode)
                {
                    apiResponse = JsonConvert.DeserializeObject<CreateMultipleResponseWebApi>(await response.Content.ReadAsStringAsync());
                }
                else
                    apiResponse = new CreateMultipleResponseWebApi() { Ids = new List<Guid>() { Guid.Empty }, ExText = await response.Content.ReadAsStringAsync() };

                return apiResponse;
            }
        }


        /// <summary>
        /// Get access token.
        /// </summary>
        /// <returns></returns>
        private static async Task<string> GetAccessToken()
        {
            var app = ConfidentialClientApplicationBuilder.Create(clientId)
                .WithClientSecret(clientSecret)
                .WithAuthority(new Uri(authority))
                .Build();

            var result = await app.AcquireTokenForClient(new[] { $"{dataverseUrl}/.default" }).ExecuteAsync();
            return result.AccessToken;
        }



        [Function("InsertCRM")]
        public async static Task<List<CrmResponse>> InsertCRM([ActivityTrigger] List<SalesRecord> salesRecords, FunctionContext ctx)
        {
            List<CrmResponse> crmResponses = new List<CrmResponse>();
            EntityCollection collection = new EntityCollection();
            List<Entity> entities = new List<Entity>();
            //var salesRecords = packet.Item1;
            var entityCollection = new Root()
            {
                Targets = new List<DurableSales>()
            };

            var logger = ctx.GetLogger(nameof(InsertCRM));
            logger.LogInformation($"trying to insert {salesRecords.Count} records");
            foreach (var salesRecord in salesRecords)
            {
                entityCollection.Targets.Add(new DurableSales()
                {
                    AdfCountry = salesRecord.Country,
                    AdfItemType = salesRecord.ItemType,
                    AdfOrderDate = salesRecord.OrderDate,
                    AdfOrderPriority = GetPriority(salesRecord.OrderPriority),
                    AdfProductName = $"{salesRecord.Region}-{salesRecord.ItemType}",
                    AdfQuantitySold = 0,
                    OrderId = salesRecord.OrderID,
                    AdfRegion = salesRecord.Region,
                    AdfSaleDate = DateTime.UtcNow,
                    AdfSalesChannel = salesRecord.SalesChannel,
                    AdfShipDate = salesRecord.ShipDate,
                    AdfTotalCost = salesRecord.TotalCost,
                    AdfTotalProfit = salesRecord.TotalProfit,
                    AdfTotalRevenue = salesRecord.TotalRevenue,
                    AdfUnitCost = salesRecord.UnitCost,
                    AdfUnitPrice = salesRecord.UnitPrice,
                    OdataType = "Microsoft.Dynamics.CRM.adf_durablesales"
                });
            }

            var apiResponse = await ExecuteMultipleDurableSales(JsonConvert.SerializeObject(entityCollection));
            logger.LogInformation(JsonConvert.SerializeObject(apiResponse));
            if (apiResponse.Ids.Any(id => id == Guid.Empty))
            {
                crmResponses.Add(new CrmResponse { IsFaulted = true, RecordId = Guid.Empty, ExceptionText = "NO RESPONSE FROM CRM" });
            }
            else
            {
                foreach (var item in apiResponse.Ids)
                {
                    crmResponses.Add(new CrmResponse { IsFaulted = false, RecordId = item });
                }
            }

            return crmResponses;
        }

        private static int GetPriority(string priority)
        {
            if (priority.Equals("M", StringComparison.OrdinalIgnoreCase))
                return 746430001;
            else if (priority.Equals("L", StringComparison.OrdinalIgnoreCase))
                return 746430000;
            else return 746430002;
        }

        [Function("GetCosmoData")]
        public async static Task<CosmosResponse> GetCosmoData([ActivityTrigger] Tuple<int, int, int, string> packet, FunctionContext ctx)
        {
            var logger = ctx.GetLogger(nameof(GetCosmoData));
            var failCounter = packet.Item1;
            var successCounter = packet.Item2;
            if (packet.Item4 == null)
                return null;

            var query = $"select * from c OFFSET {packet.Item4.Split(',')[0]} LIMIT {packet.Item3}";
            var salesRecords = new List<SalesRecord>();
            try
            {
                //if (er <= 37)
                //  throw new ArgumentException("error bro");
                var container = cosmosClient.GetContainer("applications-master", "accounts");

                var queryDefinition = new QueryDefinition(query);
                var queryIterator = container.GetItemQueryIterator<SalesRecord>(queryDefinition);
                while (queryIterator.HasMoreResults)
                {
                    var response = await queryIterator.ReadNextAsync();
                    salesRecords.AddRange(response);
                    successCounter += response.Count;
                }

                if (query != null && logger != null)
                    logger.LogInformation(query);
            }
            catch (Exception cex)
            {
                //var bc = packet.batchCount;
                failCounter += packet.Item3;
                if (logger != null)
                    logger.LogError($"cosmos error in query-- {query}, failed {failCounter}");
                return new CosmosResponse() { FailCount = failCounter, SuccessCount = successCounter, TotalRecords = new List<SalesRecord>() };
            }

            return new CosmosResponse() { FailCount = failCounter, SuccessCount = successCounter, TotalRecords = salesRecords };
            
        }

        [Function("Function1_HttpStart")]
        public static async Task<HttpResponseData> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            [DurableClient] DurableTaskClient client,
            FunctionContext executionContext)
        {

            var countOfRecords = req.Query["rc"];
            var batch = req.Query["bc"];

            ILogger logger = executionContext.GetLogger("Function1_HttpStart");
            // Function input comes from the request content.
            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
                nameof(Function1), Tuple.Create(countOfRecords, batch));

            logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            // Returns an HTTP 202 response with an instance management payload.
            // See https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-http-api#start-orchestration
            return await client.CreateCheckStatusResponseAsync(req, instanceId);
        }
    }

    public class CrmResponse
    {
        public bool? IsFaulted { get; set; }
        public string ExceptionText { get; set; }
        public Guid? RecordId { get; set; }
    }

    public class SalesRecord
    {
        public string Region { get; set; }
        public string Country { get; set; }
        public string ProductName { get; set; }
        public string ItemType { get; set; }
        public string SalesChannel { get; set; }
        public string OrderPriority { get; set; }
        public DateTime OrderDate { get; set; }
        public int OrderID { get; set; }
        public DateTime ShipDate { get; set; }
        public decimal UnitsSold { get; set; }
        public decimal UnitPrice { get; set; }
        public decimal UnitCost { get; set; }
        public decimal TotalRevenue { get; set; }
        public decimal TotalCost { get; set; }
        public decimal TotalProfit { get; set; }
    }

    public class CosmosResponse
    {
        public int SuccessCount { get; set; }
        public int FailCount { get; set; }

        public List<SalesRecord> TotalRecords { get; set; }
    }

    public class OrchastratorResponse
    {
        public Guid? OrchastrationId { get; set; }
        public int CosmosReadSuccessCount { get; set; }
        public int CosmosReadFailureCount { get; set; }
        public int CRMWriteSuccessCount { get; set; }
        public int CRMWriteFailureCount { get; set; }
        public List<string> Exceptions { get; set; }
    }

    public class DurableSales
    {
        [JsonProperty("adf_country")]
        public string AdfCountry { get; set; }

        [JsonProperty("adf_itemtype")]
        public string AdfItemType { get; set; }

        [JsonProperty("adf_orderid")]
        public int OrderId { get; set; }

        [JsonProperty("adf_orderdate")]
        public DateTime AdfOrderDate { get; set; }

        [JsonProperty("adf_orderpriority")]
        public int AdfOrderPriority { get; set; }

        [JsonProperty("adf_productname")]
        public string AdfProductName { get; set; }

        [JsonProperty("adf_quantitysold")]
        public int AdfQuantitySold { get; set; }

        [JsonProperty("adf_region")]
        public string AdfRegion { get; set; }

        [JsonProperty("adf_saledate")]
        public DateTime? AdfSaleDate { get; set; }

        [JsonProperty("adf_saleschannel")]
        public string AdfSalesChannel { get; set; }

        [JsonProperty("adf_shipdate")]
        public DateTime? AdfShipDate { get; set; }

        [JsonProperty("adf_totalcost")]
        public decimal? AdfTotalCost { get; set; }

        [JsonProperty("adf_totalprofit")]
        public decimal? AdfTotalProfit { get; set; }

        [JsonProperty("adf_totalrevenue")]
        public decimal? AdfTotalRevenue { get; set; }

        [JsonProperty("adf_unitcost")]
        public decimal? AdfUnitCost { get; set; }

        [JsonProperty("adf_unitprice")]
        public decimal? AdfUnitPrice { get; set; }

        [JsonProperty("@odata.type")]
        public string OdataType { get; set; }
    }

    public class CreateMultipleResponseWebApi
    {
        [JsonProperty("@odata.context")]
        public string OdataContext { get; set; }

        [JsonProperty("Ids")]
        public List<Guid> Ids { get; set; }

        public string ExText { get; set; }
    }

    public class Root
    {
        [JsonProperty("Targets")]
        public List<DurableSales> Targets { get; set; }
    }
}
