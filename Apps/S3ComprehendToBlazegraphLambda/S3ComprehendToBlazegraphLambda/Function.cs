using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using VDS.RDF;
using VDS.RDF.Parsing;
using VDS.RDF.Storage;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace S3ComprehendToBlazegraphLambda
{
    public class Function
    {
        /// <summary>
        /// Blazegraph endpoint URL.
        /// </summary>
        public const string BLAZEGRAPH_ENDPOINT = "http://ec2-18-197-189-202.eu-central-1.compute.amazonaws.com:9999/blazegraph/";

        public const string TEXTS_GRAPH = "http://blazegraph-webapp/texts";

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
        /// <param name="s3Event"></param>
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
                            // get text detail
                            //
                            var textDetail = JsonConvert.DeserializeObject<TextDetail>(await reader.ReadToEndAsync());

                            //
                            // load selfies graph
                            //
                            IGraph textsGraph = new Graph();
                            _blazegraph.LoadGraph(textsGraph, UriFactory.Create(TEXTS_GRAPH));

                            //
                            // create triples
                            //
                            INode textNode = null;
                            try
                            {
                                textNode = _nodeFactory.CreateUriNode(UriFactory.Create(Uri.EscapeUriString("http://blazegraph-webapp/" + textDetail.Id)));
                            }
                            catch (Exception ex)
                            {
                                context.Logger.LogLine(ex.Message);
                                return;
                            }

                            var triples = new List<Triple>();

                            // text instance is of type Text class
                            triples.Add(new Triple(
                                subj: textNode,
                                pred: _nodeFactory.CreateUriNode(UriFactory.Create(NamespaceMapper.RDF + "type")),
                                obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text"))
                            ));

                            // text instance label
                            triples.Add(new Triple(
                                subj: textNode,
                                pred: _nodeFactory.CreateUriNode(UriFactory.Create(NamespaceMapper.RDFS + "label")),
                                obj: _nodeFactory.CreateLiteralNode(textDetail.Id, new Uri(XmlSpecsHelper.XmlSchemaDataTypeString))
                            ));

                            // text instance has an entityCollection instance
                            triples.Add(new Triple(
                                subj: textNode,
                                pred: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/hasEntityCollection")),
                                obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/entityCollection/" + textDetail.Id))
                            ));

                            // entityCollection instance is of type EntityCollection class
                            triples.Add(new Triple(
                                subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/entityCollection/" + textDetail.Id)),
                                pred: _nodeFactory.CreateUriNode(UriFactory.Create(NamespaceMapper.RDF + "type")),
                                obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/entityCollection"))
                            ));

                            // filling entityCollection attributes
                            foreach (var entity in textDetail.Entities)
                            {
                                triples.Add(new Triple(
                                    subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/entityCollection/" + textDetail.Id)),
                                    pred: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/entityCollection/hasEntity")),
                                    obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/entityCollection/" + entity.Type.Value.ToLower()))
                                ));

                                // entity instance is of type Entity class
                                triples.Add(new Triple(
                                    subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/entityCollection/" + entity.Type.Value.ToLower())),
                                    pred: _nodeFactory.CreateUriNode(UriFactory.Create(NamespaceMapper.RDF + "type")),
                                    obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/entityCollection/entity"))
                                ));
                            }

                            // text instance has an sentiment instance
                            triples.Add(new Triple(
                                subj: textNode,
                                pred: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/hasSentiment")),
                                obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/sentiment/" + textDetail.Id))
                            ));

                            // sentiment instance is of type sentiment that the text has
                            triples.Add(new Triple(
                                subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/sentiment/" + textDetail.Id)),
                                pred: _nodeFactory.CreateUriNode(UriFactory.Create(NamespaceMapper.RDF + "type")),
                                obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/" + textDetail.Sentiment.Value.ToLower()))
                            ));

                            // the class sentiment that the text has is a sublcass of Sentiment class
                            triples.Add(new Triple(
                                subj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/" + textDetail.Sentiment.Value.ToLower())),
                                pred: _nodeFactory.CreateUriNode(UriFactory.Create(NamespaceMapper.RDFS + "subClassOf")),
                                obj: _nodeFactory.CreateUriNode(UriFactory.Create("http://blazegraph-webapp/text/sentiment"))
                            ));

                            //
                            // updated triples in graph
                            //
                            try
                            {
                                _blazegraph.UpdateGraph(UriFactory.Create(TEXTS_GRAPH), triples, new List<Triple>());
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