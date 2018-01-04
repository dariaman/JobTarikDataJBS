using System;
using System.Collections.Generic;
using System.Text;

namespace JobTarikDataJBS
{
    class ScheduleJobLogModel
    {
        public int id { get; set; }
        public int ScheduleJobID { get; set; }
        public string spName { get; set; }
        public DateTime ScheduleTgl { get; set; }
        public int ScheduleJam { get; set; }
    }
}
