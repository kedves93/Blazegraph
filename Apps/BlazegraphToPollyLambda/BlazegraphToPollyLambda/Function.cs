using Amazon;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Amazon.Polly;
using Amazon.Polly.Model;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using VDS.RDF.Storage;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace BlazegraphToPollyLambda
{
    public class Function
    {
        /// <summary>
        /// Blazegraph endpoint URL.
        /// </summary>
        public const string BLAZEGRAPH_ENDPOINT = "http://ec2-18-197-189-202.eu-central-1.compute.amazonaws.com:9999/blazegraph/";

        /// <summary>
        /// The name of the bucket where to upload the speech.
        /// </summary>
        public const string TARGET_BUCKET = "blazegraphwebapp-postprocess-bucket";

        /// <summary>
        /// Only allow requests from blazegraphwebapp hosted in S3
        /// </summary>
        // public const string ALLOWED_ORIGIN = "http://blazegraphwebapp.s3-website.eu-central-1.amazonaws.com/";
        public const string ALLOWED_ORIGIN = "*";

        private readonly BlazegraphConnector _blazegraph;

        private readonly IAmazonS3 _s3Client;

        private readonly IAmazonPolly _pollyClient;

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            _s3Client = new AmazonS3Client();
            _pollyClient = new AmazonPollyClient(RegionEndpoint.EUWest1);
            _blazegraph = new BlazegraphConnector(BLAZEGRAPH_ENDPOINT);
        }

        /// <summary>
        /// A function handling the APIGatewayProxyRequest
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<APIGatewayProxyResponse> FunctionHandler(APIGatewayProxyRequest request, ILambdaContext context)
        {
            var synthesizeSpeechResponse = new SynthesizeSpeechResponse();
            try
            {
                synthesizeSpeechResponse = await _pollyClient.SynthesizeSpeechAsync(new SynthesizeSpeechRequest()
                {
                    Engine = Engine.Neural,
                    LanguageCode = LanguageCode.EnUS,
                    OutputFormat = OutputFormat.Mp3,
                    SampleRate = "24000",
                    Text = "Hi, my name is Szilard. I am from Brasov and I work at Siemens.",
                    TextType = TextType.Text,
                    VoiceId = VoiceId.Joanna
                });
            }
            catch (AmazonPollyException ex)
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
            // put audio stream into S3
            //
            using (MemoryStream stream = new MemoryStream())
            {
                try
                {
                    await synthesizeSpeechResponse.AudioStream.CopyToAsync(stream);
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

                try
                {
                    await _s3Client.PutObjectAsync(new PutObjectRequest()
                    {
                        InputStream = stream,
                        BucketName = TARGET_BUCKET,
                        Key = Path.Combine("audios", DateTime.Now.ToString("dd-MM-yyyy_HH-mm-ss-ffff") + ".mp3")
                    });
                }
                catch (AmazonS3Exception ex)
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