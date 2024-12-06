# Orchestrator Function

This project contains an Azure Durable Function that orchestrates the processing of sales records and CRM responses. The function reads data from Cosmos DB and inserts it into a CRM system.

## Function Overview

### `RunOrchestrator`

This is the main orchestrator function that coordinates the workflow.

#### Parameters

- `context`: The orchestration context provided by Azure Durable Functions.

#### Workflow

1. **Input Retrieval**: The function retrieves input data (`rcbc`) as a tuple containing two strings.
2. **Logging**: Initializes a replay-safe logger.
3. **Service Configuration**: Configures the `ServicePointManager` for optimal performance.
4. **CRM Service Client**: Initializes the CRM service client.
5. **Orchestration ID**: Sets the orchestration ID in the response.
6. **Task Creation**: Creates tasks to read data from Cosmos DB based on the input parameters.
7. **Task Execution**: Executes the tasks and handles any exceptions.
8. **CRM Insertion**: Processes the data read from Cosmos DB and inserts it into the CRM system.

## Configuration

### ServicePointManager Settings

- `DefaultConnectionLimit`: Set to 65000 to allow a high number of concurrent connections.
- `Expect100Continue`: Disabled to reduce latency.
- `UseNagleAlgorithm`: Disabled to improve performance.
- `SecurityProtocol`: Set to `Tls12` for secure communication.

### CRM Service Client

- **URI**: `https://org798d1247.crm.dynamics.com/`
- **Client ID**: `e6130d5d-a165-44a9-8964-6f11ed59549f`
- **Client Secret**: `InP8Q~d9LTP1ZSQIwafRY6Lf7tAppZYEQm2cVaX_`

## Logging

The function uses a replay-safe logger to log important information and errors.

## Error Handling

The function catches `AggregateException` during task execution and logs the stack trace.

## Dependencies

- Azure Durable Functions
- Cosmos DB
- CRM Service Client

## How to Run

1. Clone the repository.
2. Open the solution in Visual Studio.
3. Configure the necessary settings (e.g., CRM service client credentials).
4. Deploy the function to Azure.

## License

This project is licensed under the MIT License.
