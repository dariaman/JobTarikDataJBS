using System;
using System.Collections.Generic;
using System.Text;

namespace JobTarikDataJBS
{
    class ScheduleJobModel
    {
        public int id {get; set;}
        public Boolean IsDaily { get; set; }
        public int? StepHour { get; set; }
        public Boolean IsMonthly { get; set; }
        public int? tgl { get; set; }
        public int? waktuExecute { get; set; }
        public string spName { get; set; }
        public Boolean IsAktif { get; set; }
        public DateTime? DateCrt { get; set; }
        public DateTime? DateUpdate { get; set; }
    }
}
