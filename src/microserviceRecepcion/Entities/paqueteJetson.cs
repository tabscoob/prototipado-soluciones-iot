
using System;
namespace microserviceRecepcion.Entities;

    public class paqueteJetson 
    {
        public string device_id {get; set;}

        public int people_total {get; set;}
        
        public int people_with_mask {get; set;}

        public int people_without_mask {get; set;}

        public double timestamp {get; set;}
    }
