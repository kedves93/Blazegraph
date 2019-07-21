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
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<APIGatewayProxyResponse> FunctionHandler(APIGatewayProxyRequest request, ILambdaContext context)
        {
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
                    Headers = new Dictionary<string, string>() { { "Content-Type", "text/plain" } }
                };
            }

            try
            {
                await _s3Client.PutObjectAsync(new PutObjectRequest()
                {
                    ContentBody = image.Base64Content,
                    BucketName = TARGET_BUCKET,
                    Key = Path.Combine("images", image.Name, image.Extension)
                });
            }
            catch (AmazonS3Exception ex)
            {
                context.Logger.LogLine(ex.Message);
                return new APIGatewayProxyResponse() { StatusCode = 500 };
            }

            return new APIGatewayProxyResponse() { StatusCode = 200 };
        }
    }
}