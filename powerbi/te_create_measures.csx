// Tabular Editor 2 — Bulk-create all DAX measures for the Olist dashboard.
// Usage: open Power BI Desktop with the model, launch Tabular Editor via External Tools,
//        paste this script into the C# Script tab and press F5, then Ctrl+S.

foreach(var m in Model.Tables["_Measures"].Measures.ToList()) m.Delete();

var t = Model.Tables["_Measures"];

var defs = new[] {
    new { Name = "Total Revenue",         Dax = "SUM('mart fact_sales'[total_value])",                                                                                                                                                                                                                                                                                                                                      Folder = "Revenue"  },
    new { Name = "Total Orders",          Dax = "DISTINCTCOUNT('mart fact_sales'[order_id])",                                                                                                                                                                                                                                                                                                                                Folder = "Revenue"  },
    new { Name = "Avg Order Value",       Dax = "DIVIDE([Total Revenue], [Total Orders])",                                                                                                                                                                                                                                                                                                                                   Folder = "Revenue"  },
    new { Name = "On-Time Delivery Rate", Dax = "DIVIDE(COUNTROWS(FILTER('mart fact_sales', NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]) && 'mart fact_sales'[delivery_vs_estimate_days] <= 0)), COUNTROWS(FILTER('mart fact_sales', NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]))))",                                                                                                              Folder = "Delivery" },
    new { Name = "Avg Review Score",      Dax = "AVERAGEX(FILTER('mart fact_sales', NOT ISBLANK('mart fact_sales'[review_score])), 'mart fact_sales'[review_score])",                                                                                                                                                                                                                                                        Folder = "Customer" },
    new { Name = "Title Revenue Trend",   Dax = "VAR _min = MIN('mart dim_date'[year])\nVAR _max = MAX('mart dim_date'[year])\nVAR _count = DISTINCTCOUNT('mart dim_date'[year])\nRETURN\n    \"Monthly Revenue Trend (R$, \" &\n    IF(_count = 1, _min, _min & \"–\" & _max) &\n    \")\"",                                                                                                                              Folder = "Display"  },
    new { Name = "Last Updated",          Dax = "VAR _date = MAX('mart fact_sales'[mart_load_ts])\nVAR _month = MONTH(_date)\nVAR _month_name = SWITCH(_month,\n    1, \"Jan\", 2, \"Feb\", 3, \"Mar\", 4, \"Apr\",\n    5, \"May\", 6, \"Jun\", 7, \"Jul\", 8, \"Aug\",\n    9, \"Sep\", 10, \"Oct\", 11, \"Nov\", 12, \"Dec\"\n)\nRETURN\n    \"Last updated: \" & FORMAT(_date, \"DD\") & \" \" & _month_name & \" \" & FORMAT(_date, \"YYYY\")", Folder = "Display"  },
};

foreach(var d in defs) {
    t.AddMeasure(d.Name, d.Dax, d.Folder);
}
