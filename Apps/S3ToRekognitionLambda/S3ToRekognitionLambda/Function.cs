using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.Rekognition;
using Amazon.Rekognition.Model;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace S3ToRekognitionLambda
{
    public class Function
    {
        /// <summary>
        /// The default minimum confidence used for detecting faces.
        /// </summary>
        public const float DEFAULT_MIN_CONFIDENCE = 70f;

        /// <summary>
        /// The name of the environment variable to set which will override the default minimum confidence level.
        /// </summary>
        public const string MIN_CONFIDENCE_ENVIRONMENT_VARIABLE_NAME = "MinConfidence";

        /// <summary>
        /// The name of the bucket where the processed image metadata should be saved.
        /// </summary>
        public const string TARGET_BUCKET = "blazegraphwebapp-postprocess-images-bucket";

        private readonly IAmazonS3 _s3Client;

        private readonly IAmazonRekognition _rekognitionClient;

        private readonly float _minConfidence;

        private readonly List<string> _supportedImageTypes;

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
            _rekognitionClient = new AmazonRekognitionClient();
            _minConfidence = DEFAULT_MIN_CONFIDENCE;
            _supportedImageTypes = new List<string> { ".png", ".jpg", ".jpeg" };

            string environmentMinConfidence = Environment.GetEnvironmentVariable(MIN_CONFIDENCE_ENVIRONMENT_VARIABLE_NAME);

            if (string.IsNullOrWhiteSpace(environmentMinConfidence))
                return;

            if (!float.TryParse(environmentMinConfidence, out float value))
                return;

            _minConfidence = value;
        }

        /// <summary>
        /// A function for responding to S3 create events. It will determine if the object is an image and use Amazon Rekognition
        /// to detect faces and save the metadata to S3.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(S3Event s3Event, ILambdaContext context)
        {
            foreach (var record in s3Event.Records)
            {
                //
                // check file extension
                //
                if (!_supportedImageTypes.Contains(Path.GetExtension(record.S3.Object.Key)))
                    continue;

                //
                // detect faces
                //
                var detectResponses = new DetectFacesResponse();
                try
                {
                    detectResponses = await _rekognitionClient.DetectFacesAsync(new DetectFacesRequest()
                    {
                        Image = new Image
                        {
                            S3Object = new Amazon.Rekognition.Model.S3Object
                            {
                                Bucket = record.S3.Bucket.Name,
                                Name = record.S3.Object.Key
                            }
                        },
                        Attributes = new List<string>() { "ALL" }
                    });
                }
                catch (Exception ex)
                {
                    context.Logger.LogLine(ex.Message);
                }

                //
                // save metadata in S3 bucket only if confidence level is good
                //
                var faceDetailsWithMinConfidence = detectResponses.FaceDetails.Where(x => x.Confidence >= _minConfidence);

                if (!faceDetailsWithMinConfidence.Any())
                    return;

                try
                {
                    await _s3Client.PutObjectAsync(new PutObjectRequest()
                    {
                        ContentBody = JsonConvert.SerializeObject(faceDetailsWithMinConfidence),
                        BucketName = TARGET_BUCKET,
                        Key = Path.Combine("images", Path.GetFileNameWithoutExtension(record.S3.Object.Key), "detections.json")
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