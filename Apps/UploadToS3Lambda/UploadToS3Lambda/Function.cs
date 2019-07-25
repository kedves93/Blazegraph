using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace UploadToS3Lambda
{
    public class Function
    {
        /// <summary>
        /// The name of the bucket where to upload the image.
        /// </summary>
        public const string TARGET_BUCKET = "blazegraphwebapp-preprocess-images-bucket";

        /// <summary>
        /// Only allow requests from blazegraphwebapp hosted in S3
        /// </summary>
        // public const string ALLOWED_ORIGIN = "http://blazegraphwebapp.s3-website.eu-central-1.amazonaws.com/";
        public const string ALLOWED_ORIGIN = "*";

        private readonly IAmazonS3 _s3Client;

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            _s3Client = new AmazonS3Client();
        }

        /// <summary>
        /// A function handling the APIGatewayProxyRequest
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<APIGatewayProxyResponse> FunctionHandler(APIGatewayProxyRequest request, ILambdaContext context)
        {
            //
            // get image from request
            //
            Image image = new Image();

            try
            {
                image = JsonConvert.DeserializeObject<Image>(request.Body);
            }
            catch (Exception ex)
            {
                return new APIGatewayProxyResponse()
                {
                    StatusCode = 400,
                    Body = ex.Message,
                    Headers = new Dictionary<string, string>()
                    {
                        { "Content-Type", "text/plain" },
                        { "Access-Control-Allow-Origin", ALLOWED_ORIGIN }
                    }
                };
            }

            //
            // create stream
            //
            MemoryStream stream;

            try
            {
                byte[] data = Convert.FromBase64String(image.Base64Content);
                stream = new MemoryStream(data);
            }
            catch (Exception ex)
            {
                context.Logger.LogLine(ex.Message);
                return new APIGatewayProxyResponse()
                {
                    StatusCode = 400,
                    Body = ex.Message,
                    Headers = new Dictionary<string, string>()
                    {
                        { "Content-Type", "text/plain" },
                        { "Access-Control-Allow-Origin", ALLOWED_ORIGIN }
                    }
                };
            }

            //
            // put stream into S3
            //
            try
            {
                await _s3Client.PutObjectAsync(new PutObjectRequest()
                {
                    InputStream = stream,
                    BucketName = TARGET_BUCKET,
                    Key = Path.Combine("images", image.Name)
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
            finally
            {
                stream.Dispose();
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