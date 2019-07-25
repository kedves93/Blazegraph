using Amazon.Rekognition.Model;
using System.Collections.Generic;

namespace S3ToRekognitionLambda
{
    public class SelfieDetail
    {
        public string ImageName { get; set; }

        public IEnumerable<Label> Labels { get; set; }

        public IEnumerable<FaceDetail> FacesDetails { get; set; }
    }
}