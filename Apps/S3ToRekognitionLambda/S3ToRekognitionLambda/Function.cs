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
        /// The default maximum number of labels to detect.
        /// </summary>
        public const int DEFAULT_MAX_LABELS = 20;

        /// <summary>
        /// The name of the environment variable to set which will override the default minimum confidence level.
        /// </summary>
        public const string MIN_CONFIDENCE_ENVIRONMENT_VARIABLE_NAME = "MinConfidence";

        /// <summary>
        /// The name of the environment variable to set which will override the default maximum number of labels.
        /// </summary>
        public const string MAX_LABELS_ENVIRONMENT_VARIABLE_NAME = "MaxLabels";

        /// <summary>
        /// The name of the bucket where the processed image metadata should be saved.
        /// </summary>
        public const string TARGET_BUCKET = "blazegraphwebapp-postprocess-bucket";

        private readonly IAmazonS3 _s3Client;

        private readonly IAmazonRekognition _rekognitionClient;

        private readonly float _minConfidence;

        private readonly int _maxLabels;

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
            if (!string.IsNullOrWhiteSpace(environmentMinConfidence) && float.TryParse(environmentMinConfidence, out float minConfidence))
                _minConfidence = minConfidence;

            string environmentMaxLabels = Environment.GetEnvironmentVariable(MAX_LABELS_ENVIRONMENT_VARIABLE_NAME);
            if (!string.IsNullOrWhiteSpace(environmentMaxLabels) && int.TryParse(environmentMaxLabels, out int maxLabels))
                _maxLabels = maxLabels;
        }

        /// <summary>
        /// A function for responding to S3 create events. It will determine if the object is an image and use Amazon Rekognition
        /// to detect labels and face details, faces and save them to S3.
        /// </summary>
        /// <param name="s3Event"></param>
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
                // detect labels
                //
                var detectLablesResponse = new DetectLabelsResponse();
                try
                {
                    detectLablesResponse = await _rekognitionClient.DetectLabelsAsync(new DetectLabelsRequest
                    {
                        Image = new Image()
                        {
                            S3Object = new Amazon.Rekognition.Model.S3Object()
                            {
                                Bucket = record.S3.Bucket.Name,
                                Name = record.S3.Object.Key
                            },
                        },
                        MaxLabels = _maxLabels,
                        MinConfidence = _minConfidence
                    });
                }
                catch (AmazonRekognitionException ex)
                {
                    context.Logger.LogLine("Error in detecting labels.");
                    context.Logger.LogLine(ex.Message);
                    return;
                }

                //
                // detect faces
                //
                var detectFacesResponse = new DetectFacesResponse();
                try
                {
                    detectFacesResponse = await _rekognitionClient.DetectFacesAsync(new DetectFacesRequest()
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
                catch (AmazonRekognitionException ex)
                {
                    context.Logger.LogLine("Error in detecting faces.");
                    context.Logger.LogLine(ex.Message);
                    return;
                }

                //
                // save detections in S3 bucket
                //
                try
                {
                    var body = new SelfieDetail()
                    {
                        ImageName = Path.GetFileNameWithoutExtension(record.S3.Object.Key),
                        Labels = detectLablesResponse.Labels,
                        FacesDetails = detectFacesResponse.FaceDetails
                    };

                    await _s3Client.PutObjectAsync(new PutObjectRequest()
                    {
                        ContentBody = JsonConvert.SerializeObject(body),
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