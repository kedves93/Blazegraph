using Amazon.Comprehend;
using Amazon.Comprehend.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Threading.Tasks;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace S3ToComprehendLambda
{
    public class Function
    {
        /// <summary>
        /// The name of the bucket where the processed text metadata should be saved.
        /// </summary>
        public const string TARGET_BUCKET = "blazegraphwebapp-postprocess-bucket";

        private readonly IAmazonS3 _s3Client;

        private readonly IAmazonComprehend _comprehendClient;

        /// <summary>
        /// <para>
        /// Default constructor used by AWS Lambda to construct the function. Credentials and Region information will
        /// be set by the running Lambda environment.
        /// </para>
        /// <para>
        /// This constuctor will also search for the environment variable overriding the default minimum confidence level
        /// for label detection.
        /// </para>
        /// </summary>
        public Function()
        {
            _s3Client = new AmazonS3Client();
            _comprehendClient = new AmazonComprehendClient();
        }

        /// <summary>
        /// A function for responding to S3 create events. It uses Amazon Comprehend to detect entities, sentiment
        /// and save them to S3.
        /// </summary>
        /// <param name="s3Event"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(S3Event s3Event, ILambdaContext context)
        {
            foreach (var record in s3Event.Records)
            {
                var detectEntitiesResponse = new DetectEntitiesResponse();
                var detectSentimentResponse = new DetectSentimentResponse();
                try
                {
                    using (GetObjectResponse response = await _s3Client.GetObjectAsync(new GetObjectRequest()
                    {
                        BucketName = record.S3.Bucket.Name,
                        Key = record.S3.Object.Key
                    }))
                    {
                        using (StreamReader reader = new StreamReader(response.ResponseStream))
                        {
                            string text = await reader.ReadToEndAsync();

                            //
                            // Detect entities
                            //
                            try
                            {
                                detectEntitiesResponse = await _comprehendClient.DetectEntitiesAsync(new DetectEntitiesRequest
                                {
                                    LanguageCode = LanguageCode.En,
                                    Text = text
                                });
                            }
                            catch (AmazonComprehendException ex)
                            {
                                context.Logger.LogLine("Error in detecting entities.");
                                context.Logger.LogLine(ex.Message);
                                return;
                            }

                            //
                            // Detect sentiment
                            //
                            try
                            {
                                detectSentimentResponse = await _comprehendClient.DetectSentimentAsync(new DetectSentimentRequest
                                {
                                    LanguageCode = LanguageCode.En,
                                    Text = text
                                });
                            }
                            catch (AmazonComprehendException ex)
                            {
                                context.Logger.LogLine("Error in detecting sentiment.");
                                context.Logger.LogLine(ex.Message);
                                return;
                            }
                        }
                    }
                }
                catch (AmazonS3Exception ex)
                {
                    context.Logger.LogLine(ex.Message);
                }

                //
                // save detections in S3 bucket
                //
                try
                {
                    var body = new TextDetail()
                    {
                        Id = Guid.NewGuid().ToString(),
                        Entities = detectEntitiesResponse.Entities,
                        Sentiment = detectSentimentResponse.Sentiment
                    };

                    await _s3Client.PutObjectAsync(new PutObjectRequest()
                    {
                        ContentBody = JsonConvert.SerializeObject(body),
                        BucketName = TARGET_BUCKET,
                        Key = Path.Combine("texts", Path.GetFileNameWithoutExtension(record.S3.Object.Key), "detections.json")
                    });
                }
                catch (AmazonS3Exception ex)
                {
                    context.Logger.LogLine(ex.Message);
                }
            }
        }
    }
}