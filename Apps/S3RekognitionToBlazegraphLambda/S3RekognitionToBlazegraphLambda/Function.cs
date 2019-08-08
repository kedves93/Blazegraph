using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using VDS.RDF;
using VDS.RDF.Parsing;
using VDS.RDF.Storage;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace S3RekognitionToBlazegraphLambda
{
    public class Function
    {
        /// <summary>
        /// Blazegraph endpoint URL.
        /// </summary>
        public const string BLAZEGRAPH_ENDPOINT = "http://ec2-18-197-189-202.eu-central-1.compute.amazonaws.com:9999/blazegraph/";

        public const string SELFIES_GRAPH = "http://blazegraph-webapp/selfies";

        private readonly IAmazonS3 _s3Client;

        private readonly BlazegraphConnector _blazegraph;

        private readonly INodeFactory _nodeFactory;

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            _s3Client = new AmazonS3Client();
            _blazegraph = new BlazegraphConnector(BLAZEGRAPH_ENDPOINT);
            _nodeFactory = new NodeFactory();
        }

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(S3Event s3Event, ILambdaContext context)
        {
            foreach (var record in s3Event.Records)
            {
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
                            //
                            // get main face
                            //
                            var selfieDetail = JsonConvert.DeserializeObject<SelfieDetail>(await reader.ReadToEndAsync());
                            var mainFace = selfieDetail.FacesDetails.OrderByDescending(faceDetail => faceDetail.BoundingBox.Width * faceDetail.BoundingBox.Height).First();

                            //
                            // load selfies graph
                            //
                            IGraph selfiesGraph = new Graph();
                            _blazegraph.LoadGraph(selfiesGraph, UriFactory.Create(SELFIES_GRAPH));

                            //
                            // create triples
                            //
                            INode imageNode = null;
                            try
                            {
                                imageNode = _nodeFactory.CreateUriNode(UriFactory.Create(Uri.EscapeUriString("http://blazegraph-webapp/" + selfieDetail.ImageName)));
                            }
                            catch (Exception ex)
                            {
                                context.Logger.LogLine(ex.Message);
                                return;
                            }

                            var triples = new List<Triple>();

                            // image instance is of type Selfie class
                            triples.Add(new Triple(
                                subj: imageNode,
                                pred: _nodeFactory.CreateUriNode(UriFactory.Create(NamespaceMapper.RDF + "type")),
                                obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie"))
                            ));

                            // image instance label
                            triples.Add(new Triple(
                                subj: imageNode,
                                pred: _nodeFactory.CreateUriNode(UriFactory.Create(NamespaceMapper.RDFS + "label")),
                                obj: _nodeFactory.CreateLiteralNode(selfieDetail.ImageName, new Uri(XmlSpecsHelper.XmlSchemaDataTypeString))
                            ));

                            // image instance has a faceDetail instance
                            triples.Add(new Triple(
                                subj: imageNode,
                                pred: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/hasFaceDetail")),
                                obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail/" + selfieDetail.ImageName))
                            ));

                            // faceDetail instance is of type FaceDetail class
                            triples.Add(new Triple(
                                subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail/" + selfieDetail.ImageName)),
                                pred: _nodeFactory.CreateUriNode(UriFactory.Create(NamespaceMapper.RDF + "type")),
                                obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail"))
                            ));

                            // filling faceDetail attributes
                            triples.AddRange(new List<Triple>()
                            {
                                new Triple(
                                    subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail/" + selfieDetail.ImageName)),
                                    pred: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail/isGender")),
                                    obj: _nodeFactory.CreateLiteralNode(mainFace.Gender.Value.ToString().ToLower(), new Uri(XmlSpecsHelper.XmlSchemaDataTypeString))
                                ),
                                new Triple(
                                    subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail/" + selfieDetail.ImageName)),
                                    pred: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail/hasMinAge")),
                                    obj: _nodeFactory.CreateLiteralNode(mainFace.AgeRange.Low.ToString(), new Uri(XmlSpecsHelper.XmlSchemaDataTypeInteger))
                                ),
                                new Triple(
                                    subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail/" + selfieDetail.ImageName)),
                                    pred: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail/hasMaxAge")),
                                    obj: _nodeFactory.CreateLiteralNode(mainFace.AgeRange.High.ToString(), new Uri(XmlSpecsHelper.XmlSchemaDataTypeInteger))
                                ),
                                new Triple(
                                    subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail/" + selfieDetail.ImageName)),
                                    pred: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail/isSmiling")),
                                    obj: _nodeFactory.CreateLiteralNode(Convert.ToInt32(mainFace.Smile.Value).ToString(), new Uri(XmlSpecsHelper.XmlSchemaDataTypeBoolean))
                                ),
                                new Triple(
                                    subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail/" + selfieDetail.ImageName)),
                                    pred: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail/hasSunglasses")),
                                    obj: _nodeFactory.CreateLiteralNode(Convert.ToInt32(mainFace.Sunglasses.Value).ToString(), new Uri(XmlSpecsHelper.XmlSchemaDataTypeBoolean))
                                ),
                                new Triple(
                                    subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail/" + selfieDetail.ImageName)),
                                    pred: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/faceDetail/isFeeling")),
                                    obj: _nodeFactory.CreateLiteralNode(mainFace.Emotions.OrderByDescending(emotion => emotion.Confidence).First().Type.ToString().ToLower(),
                                        new Uri(XmlSpecsHelper.XmlSchemaDataTypeString))
                                )
                            });

                            // image instance has scene instance
                            triples.Add(new Triple(
                                subj: imageNode,
                                pred: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/hasScene")),
                                obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/scene/" + selfieDetail.ImageName))
                            ));

                            // scene instance is of type Scene class
                            triples.Add(new Triple(
                                subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/scene/" + selfieDetail.ImageName)),
                                pred: _nodeFactory.CreateUriNode(UriFactory.Create(NamespaceMapper.RDF + "type")),
                                obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/scene"))
                            ));

                            // filling scence attributes
                            foreach (var label in selfieDetail.Labels)
                            {
                                triples.Add(new Triple(
                                    subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/scene/" + selfieDetail.ImageName)),
                                    pred: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/scene/isDescribedBy")),
                                    obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/scene/" + label.Name))
                                ));
                                foreach (var parentLabel in label.Parents)
                                {
                                    triples.Add(new Triple(
                                        subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/scene/" + label.Name)),
                                        pred: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/scene/hasParent")),
                                        obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/selfie/scene/" + parentLabel.Name))
                                    ));
                                }
                            }

                            //
                            // updated triples in graph
                            //
                            try
                            {
                                _blazegraph.UpdateGraph(UriFactory.Create(SELFIES_GRAPH), triples, new List<Triple>());
                                context.Logger.LogLine("Updated triples successfully.");
                            }
                            catch (Exception ex)
                            {
                                context.Logger.LogLine(ex.Message);
                                context.Logger.LogLine(ex.InnerException.Message);
                                return;
                            }
                        }
                    }
                }
                catch (AmazonS3Exception ex)
                {
                    context.Logger.LogLine(ex.Message);
                }
            }
        }
    }
}