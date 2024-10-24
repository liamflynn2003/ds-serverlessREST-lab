import { Handler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  QueryCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: Handler = async (event) => {
  try {
    // Print Event
    console.log("[EVENT]", JSON.stringify(event));

    const parameters = event?.pathParameters;
    const queryParams = event.queryStringParameters;
    const movieId = parameters?.movieId ? parseInt(parameters.movieId) : undefined;

    if (!movieId) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ Message: "Missing movie Id" }),
      };
    }

    // Fetch movie metadata
    const commandOutput = await ddbDocClient.send(
      new GetCommand({
        TableName: process.env.TABLE_NAME,
        Key: { id: movieId },
      })
    );

    console.log("GetCommand response: ", commandOutput);
    
    if (!commandOutput.Item) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ Message: "Invalid movie Id" }),
      };
    }

    const body = {
      data: commandOutput.Item,
    };

    // Fetch cast data
    if (queryParams?.cast === 'true') {
      const commandInput: QueryCommandInput = {
        TableName: process.env.TABLE_NAME,
      };

      if ("roleName" in queryParams) {
        commandInput.IndexName = "roleIx";
        commandInput.KeyConditionExpression = "movieId = :m and begins_with(roleName, :r)";
        commandInput.ExpressionAttributeValues = {
          ":m": movieId,
          ":r": queryParams.roleName,
        };
      } else if ("actorName" in queryParams) {
        commandInput.KeyConditionExpression = "movieId = :m and begins_with(actorName, :a)";
        commandInput.ExpressionAttributeValues = {
          ":m": movieId,
          ":a": queryParams.actorName,
        };
      } else {
        commandInput.KeyConditionExpression = "movieId = :m";
        commandInput.ExpressionAttributeValues = {
          ":m": movieId,
        };
      }

      const castCommandOutput = await ddbDocClient.send(new QueryCommand(commandInput));
      body.data.cast = castCommandOutput.Items || [];
    }

    // Return Response
    return {
      statusCode: 200,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
    };
  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error: error.message || "Internal Server Error" }),
    };
  }
};

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
