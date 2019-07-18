using BlazegraphWebApp.Interfaces;
using System;
using VDS.RDF;
using VDS.RDF.Parsing;
using VDS.RDF.Storage;

namespace BlazegraphWebApp.Services
{
    public class BlazegraphService : IBlazegraphService
    {
        public BlazegraphService()
        {
            BlazegraphConnector connector = new BlazegraphConnector("http://localhost:9999/blazegraph/");

            Graph newGraph = new Graph
            {
                BaseUri = UriFactory.Create("http://example/bookStore")
            };

            Triple triple = new Triple(
                subj: newGraph.CreateUriNode(UriFactory.Create("http://example/book1")),
                pred: newGraph.CreateUriNode(UriFactory.Create("http://example.org/ns#price")),
                obj: newGraph.CreateLiteralNode("42", new Uri(XmlSpecsHelper.XmlSchemaDataTypeInteger))
            );
            newGraph.Assert(triple);

            connector.SaveGraph(newGraph);
        }
    }
}