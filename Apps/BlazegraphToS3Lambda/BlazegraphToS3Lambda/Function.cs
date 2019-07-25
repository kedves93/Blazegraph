using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using VDS.RDF;
using VDS.RDF.Parsing;
using VDS.RDF.Storage;
using VDS.RDF.Writing;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace BlazegraphToS3Lambda
{
    public class Function
    {
        /// <summary>
        /// Blazegraph endpoint URL.
        /// </summary>
        public const string BLAZEGRAPH_ENDPOINT = "http://ec2-18-197-189-202.eu-central-1.compute.amazonaws.com:9999/blazegraph/";

        /// <summary>
        /// The name of the bucket where to export.
        /// </summary>
        public const string TARGET_BUCKET = "blazegraphwebapp-exports-bucket";

        /// <summary>
        /// Only allow requests from blazegraphwebapp hosted in S3
        /// </summary>
        // public const string ALLOWED_ORIGIN = "http://blazegraphwebapp.s3-website.eu-central-1.amazonaws.com/";
        public const string ALLOWED_ORIGIN = "*";

        public const string SELFIES_GRAPH = "http://blazegraph-webapp/selfies";

        private readonly IAmazonS3 _s3Client;

        private readonly BlazegraphConnector _blazegraph;

        private readonly List<string> _supportedExportTypes;

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            _s3Client = new AmazonS3Client();
            _blazegraph = new BlazegraphConnector(BLAZEGRAPH_ENDPOINT);
            _supportedExportTypes = new List<string> { "ttl", "rdf" };
        }

        /// <summary>
        /// A function handling the APIGatewayProxyRequest
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<APIGatewayProxyResponse> FunctionHandler(APIGatewayProxyRequest request, ILambdaContext context)
        {
            if (!_supportedExportTypes.Contains(request.QueryStringParameters["exportType"]))
            {
                return new APIGatewayProxyResponse()
                {
                    StatusCode = 400,
                    Body = "Invalid export type.",
                    Headers = new Dictionary<string, string>()
                    {
                        { "Access-Control-Allow-Origin", ALLOWED_ORIGIN }
                    }
                };
            }

            //
            // load selfies graph
            //
            IGraph selfiesGraph = new Graph();
            _blazegraph.LoadGraph(selfiesGraph, UriFactory.Create(SELFIES_GRAPH));

            //
            // create stream
            //
            try
            {
                using (MemoryStream stream = new MemoryStream())
                {
                    using (StreamWriter writer = new StreamWriter(stream))
                    {
                        switch (request.QueryStringParameters["exportType"])
                        {
                            case "ttl":
                                CompressingTurtleWriter turtleWriter = new CompressingTurtleWriter(TurtleSyntax.W3C)
                                {
                                    PrettyPrintMode = true
                                };
                                turtleWriter.Save(g: selfiesGraph, output: writer, leaveOpen: true);
                                break;

                            case "rdf":
                                PrettyRdfXmlWriter rdfXmlWriter = new PrettyRdfXmlWriter()
                                {
                                    PrettyPrintMode = true
                                };
                                rdfXmlWriter.Save(g: selfiesGraph, output: writer, leaveOpen: true);
                                break;
                        }

                        //
                        // put stream into S3
                        //
                        await _s3Client.PutObjectAsync(new PutObjectRequest()
                        {
                            InputStream = stream,
                            BucketName = TARGET_BUCKET,
                            Key = DateTime.Now.ToString("yyyy-MM-dd hh-mm-ss", DateTimeFormatInfo.InvariantInfo) + "." + request.QueryStringParameters["exportType"]
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                context.Logger.LogLine(ex.Message);
                return new APIGatewayProxyResponse()
                {
                    StatusCode = 500,
                    Body = ex.Message,
                    Headers = new Dictionary<string, string>()
                    {
                        { "Content-Type", "text/plain" },
                        { "Access-Control-Allow-Origin", ALLOWED_ORIGIN }
                    }
                };
            }

            //
            //  return successfull response
            //
            return new APIGatewayProxyResponse()
            {
                StatusCode = 200,
                Headers = new Dictionary<string, string>()
                {
                    { "Access-Control-Allow-Origin", ALLOWED_ORIGIN }
                }
            };
        }
    }
}