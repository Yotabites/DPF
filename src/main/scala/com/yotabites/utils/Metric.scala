package com.yotabites.utils

case class Metric(
                   project: String,
                   start_tm: String,
                   end_tm: String,
                   total_tm: String,
                   load_cnt: String,
                   load_tms: String,
                   prev_checkpoint: String,
                   checkpoint: String,
                   misc: String
                 )
