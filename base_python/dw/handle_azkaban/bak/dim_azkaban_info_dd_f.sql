drop table if exists dim_azkaban_info_dd_f;
create table dim_azkaban_info_dd_f(
id bigserial
,project_id varchar(255)
,project_name varchar(255)
,project_desc varchar(255)
,flow_id varchar(255)
,cron_expression varchar(255)
,create_time timestamp(6) DEFAULT ('now'::text)::timestamp(0) with time zone
,last_upd_time timestamp(6) DEFAULT ('now'::text)::timestamp(0) with time zone
)
;
COMMENT ON TABLE "public"."dim_azkaban_info_dd_f" IS 'Azkaban调度任务的元数据信息';
COMMENT ON COLUMN "public"."dim_azkaban_info_dd_f"."id" IS 'id自增';
COMMENT ON COLUMN "public"."dim_azkaban_info_dd_f"."project_id" is '项目id';
COMMENT ON COLUMN "public"."dim_azkaban_info_dd_f"."project_name" is '项目名称';
COMMENT ON COLUMN "public"."dim_azkaban_info_dd_f"."project_desc" is '项目名称';
COMMENT ON COLUMN "public"."dim_azkaban_info_dd_f"."flow_id" is '项目名称';
COMMENT ON COLUMN "public"."dim_azkaban_info_dd_f"."cron_expression" is '项目名称';
COMMENT ON COLUMN "public"."dim_azkaban_info_dd_f"."create_time" IS '创建时间';
COMMENT ON COLUMN "public"."dim_azkaban_info_dd_f"."last_upd_time" IS '最后修改时间';