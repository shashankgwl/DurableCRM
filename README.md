# DurableCRMFunction

This project contains an Azure Durable Function that orchestrates the process of reading data from Cosmos DB and inserting it into a CRM system.

## Overview

The `DurableCRMFunction` class is the main entry point for the orchestration. It performs the following steps:
1. Reads input parameters.
2. Retrieves queries from Cosmos DB.
3. Executes the queries and processes the results.
4. Handles any failed queries by retrying them after a delay.
5. Inserts the retrieved data into the CRM system.

## Prerequisites

- Azure Functions SDK
- Cosmos DB account
- CRM system credentials

## Configuration

The following configuration values are required:
- `CosmosClient`: The Cosmos DB client instance.
- `clientId`: The client ID for authentication.
- `clientSecret`: The client secret for authentication.
- `authority`: The authority URL for authentication.
- `dataverseUrl`: The URL of the CRM system.

## Functions

### RunOrchestrator

The main orchestrator function that coordinates the entire process.

#### Input
- `Tuple<string, string, string>`: Contains the count of records, batch size, and start position.

#### Output
- `OrchastratorResponse`: Contains the results of the orchestration, including success and failure counts.

### GetCosmoQueries

An activity function that retrieves the queries to be executed against Cosmos DB.

#### Input
- `Tuple<string, string, string>`: Contains the count of records, batch size, and start position.

#### Output
- `List<string>`: A list of queries to be executed.

### GetCosmoData

An activity function that executes a query against Cosmos DB and retrieves the data.

#### Input
- `string`: The query to be executed.

#### Output
- `CosmosResponse`: Contains the results of the query execution.

### InsertCRM

An activity function that inserts data into the CRM system.

#### Input
- `List<SalesRecord>`: The data to be inserted.

#### Output
- `List<CrmResponse>`: The results of the CRM insert operation.

## Logging

The function uses a replay-safe logger to log information during the orchestration process.

## Error Handling

Failed queries are retried after a delay of 10 minutes. If the retry also fails, the failure is logged and counted.

## Example

```csharp
var rcbc = new Tuple<string, string, string>("100", "10", "0");
var response = await context.CallSubOrchestratorAsync<OrchastratorResponse>(nameof(RunOrchestrator), rcbc);
