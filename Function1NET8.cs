namespace DurableCRM
{
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Functions.Worker;
    using Microsoft.Azure.Functions.Worker.Http;
    using Microsoft.DurableTask;
    using Microsoft.DurableTask.Client;
    using Microsoft.Extensions.Logging;
    using Microsoft.Identity.Client;
    using Newtonsoft.Json;
    using System.Net.Http.Headers;
    using System.Text;
    using WorkerRuntime = Microsoft.Azure.Functions.Worker;

    public static class DurableCRMFunction
    {
        static CosmosClient cosmosClient = new CosmosClient(Helper.GetCosmoString);
        private static string clientId = Helper.GetClientId;
        private static string clientSecret = Helper.GetClientSecret;
        private static string authority = "https://login.microsoftonline.com/efe64105-431e-4df2-abda-608a635e081f";
        private static string dataverseUrl = "https://org798d1247.crm.dynamics.com";

        [Function(nameof(RunOrchestrator))]
        public static async Task<OrchastratorResponse?> RunOrchestrator(
    [WorkerRuntime.OrchestrationTrigger] TaskOrchestrationContext context)
        {
            ILogger? logger = null;
            var finalResponse = new OrchastratorResponse();

            try
            {
                var rcbc = context.GetInput<Tuple<string, string, string>>();

                var finalSales = new List<SalesRecord>();
                var finalCrmResponse = new List<CrmResponse>();
                logger = context.CreateReplaySafeLogger(nameof(RunOrchestrator));

                await LogInformation(context, logger, $"value of received parameters is countOfRecords={rcbc.Item1} batch={rcbc.Item2} startFrom={rcbc.Item3}");

                finalResponse.OrchastrationId = Guid.Parse(context.InstanceId);

                var queries = await context.CallActivityAsync<List<string>>(nameof(GetCosmoQueries), rcbc);

                var cosmosTasks = queries.Select(query => context.CallActivityAsync<CosmosResponse>(nameof(GetCosmoData), query)).ToList();

                await Task.WhenAll(cosmosTasks);

                finalResponse.CosmosReadSuccessCount = cosmosTasks.Count(task => !task.Result.IsFaulted) * int.Parse(rcbc.Item2);

                List<Task<List<CrmResponse>>> crmTasks = new List<Task<List<CrmResponse>>>();
                var failedQueries = cosmosTasks.Where(task => task.Result.IsFaulted).Select(task => task.Result.Query).ToList();
                if (failedQueries.Any())
                {
                    await LogInformation(context, logger, $"now trying failed tasks after 10 mins, total failed tasks {failedQueries.Count()}");

                    await context.CreateTimer(TimeSpan.FromMinutes(10), CancellationToken.None);

                    var failedCosmosTasks = failedQueries.Select(query => context.CallActivityAsync<CosmosResponse>(nameof(GetCosmoData), query)).ToList();

                    await Task.WhenAll(failedCosmosTasks);

                    failedCosmosTasks.ForEach(action: async t =>
                    {
                        if (t.Result.TotalRecords.Count > 0)
                            crmTasks.Add(context.CallActivityAsync<List<CrmResponse>>(nameof(InsertCRM), t.Result.TotalRecords));
                        else
                            await LogInformation(context, logger, $"skipping CRM insert because of 0 records");
                    });

                    finalResponse.CosmosReadSuccessCount += failedCosmosTasks.Count(task => !task.Result.IsFaulted) * int.Parse(rcbc.Item2);
                    finalResponse.CosmosReadFailureCount = failedCosmosTasks.Count(task => task.Result.IsFaulted) * int.Parse(rcbc.Item2);
                }

                cosmosTasks.ForEach(action: async t =>
                {
                    if (t.Result.TotalRecords.Count > 0)
                        crmTasks.Add(context.CallActivityAsync<List<CrmResponse>>(nameof(InsertCRM), t.Result.TotalRecords));
                    else
                        await LogInformation(context, logger, $"skipping CRM insert because of 0 records");
                });



                await Task.WhenAll(crmTasks);

                finalResponse.CRMWriteSuccessCount = crmTasks.Count(task => !task.IsFaulted) * int.Parse(rcbc.Item2);
                finalResponse.CRMWriteFailureCount = crmTasks.Count(task => task.IsFaulted) * int.Parse(rcbc.Item2);


                return finalResponse;
            }

            catch (TaskFailedException ex)
            {
                logger.LogError($"Orchastration {context.InstanceId} failed with error {Environment.NewLine}{ex.Message}");
                finalResponse.CRMWriteSuccessCount = 0;
                finalResponse.CRMWriteFailureCount = 0;
                finalResponse.CosmosReadFailureCount = 0;
                return finalResponse;
            }
        }

        public async static Task LogInformation(TaskOrchestrationContext context, ILogger logger, string logString)
        {
            if (context != null)
            {
                if (!context.IsReplaying)
                {
                    logger.LogInformation(logString);
                }
            }
        }

        [Function("GetCosmoQueries")]
        public static List<string> GetCosmoQueries([ActivityTrigger] Tuple<string, string, string> rcbc, FunctionContext ctx)
        {
            var logger = ctx.GetLogger<ILogger>();
            var listOfQueries = new List<string>();

            try
            {
                var recCount = int.Parse(rcbc.Item1);
                var batchCount = int.Parse(rcbc.Item2);
                var startFrom = int.Parse(rcbc.Item3);

                for (int i = startFrom; i < recCount + startFrom; i += batchCount)
                {
                    if (null != logger)
                        logger.LogInformation($"select * from c OFFSET {i} LIMIT {batchCount}");
                    listOfQueries.Add($"select * from c OFFSET {i} LIMIT {batchCount}");
                }
            }
            catch (Exception ex)
            {
                logger.LogError($"GetCosmoQueries Error {ex.Message}");
            }

            return listOfQueries;
        }



        private static async Task<CreateMultipleResponseWebApi> ExecuteMultipleDurableSales(string jsonPayload)
        {
            CreateMultipleResponseWebApi? apiResponse = null;

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
        public async static Task<CosmosResponse> GetCosmoData([ActivityTrigger] string query, FunctionContext ctx)
        {
            var logger = ctx.GetLogger(nameof(GetCosmoData));
            //var failCounter = packet.Item1;
            //var successCounter = packet.Item2;
            //if (packet.Item4 == null)
            //  /  return null;

            //var query = $"select * from c OFFSET {packet.Item4.Split(',')[0]} LIMIT {packet.Item3}";
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
                    //successCounter += response.Count;
                }

                if (query != null && logger != null)
                    logger.LogInformation(query);
            }

            catch (CosmosException cex)
            {
                if (logger != null)
                    logger.LogError($"cosmos 429 error setting retry after 10 minutes.");
                return new CosmosResponse() { IsFaulted = true, Query = query, SuccessCount = 0, TotalRecords = new List<SalesRecord>() };
            }
            catch (Exception cex)
            {
                //var bc = packet.batchCount;
                //failCounter += packet.Item3;
                if (logger != null)
                    logger.LogError($"cosmos error in query-- {query}, {cex.Message}{Environment.NewLine}{cex.StackTrace}");
                return new CosmosResponse() { IsFaulted = true, Query = query, SuccessCount = 0, TotalRecords = new List<SalesRecord>() };
            }

            return new CosmosResponse() { IsFaulted = false, SuccessCount = salesRecords.Count, TotalRecords = salesRecords };

        }



        [Function("Function1_HttpStart")]
        public static async Task<HttpResponseData> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            [WorkerRuntime.DurableClient] DurableTaskClient client,
            FunctionContext executionContext)
        {
            var countOfRecords = req.Query["numberOfRecords"];
            var batch = req.Query["batchSize"];
            var startFrom = req.Query["startFrom"];
            string instanceId;
            if (null == countOfRecords || null == batch || null == startFrom)
            {
                var resp = req.CreateResponse(System.Net.HttpStatusCode.NotFound);
                await resp.WriteStringAsync("Missing query string parameters, numberOfRecords or batchSize or startFrom.");
                return resp;
            }

            else
            {
                ILogger logger = executionContext.GetLogger($" started {countOfRecords} {batch} {startFrom}");
                instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
                nameof(RunOrchestrator), Tuple.Create(countOfRecords, batch, startFrom));
            }

            // Function input comes from the request content.


            //logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            // Returns an HTTP 202 response with an instance management payload.
            // See https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-http-api#start-orchestration
            //return await client.CreateCheckStatusResponseAsync(req, instanceId);
            return await client.CreateCheckStatusResponseAsync(req, instanceId);
        }
    }

    #region models

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
        public bool IsFaulted { get; set; }
        public string? Query { get; set; }
        //public TimeSpan? RetryAfter { get; set; }
        public List<SalesRecord>? TotalRecords { get; set; }
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

    #endregion
}
