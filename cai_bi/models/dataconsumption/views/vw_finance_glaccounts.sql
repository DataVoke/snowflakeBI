{{
    config(
        materialized="view",
        schema="dataconsumption",
        alias="vw_finance_glaccounts"
    )
}}

with 
    glaccounts as (select * from {{ source("sage_intacct_gl","gl_account") }} where _fivetran_deleted = false)

select 
    cast(gl.recordno as varchar(5000)) as key,
    cast(gl.categorykey as varchar(5000)) as key_category,
    cast(gl.closetoacctkey as varchar(5000)) as key_account_close_to,
    cast(gl.createdby as varchar(5000)) as src_created_by,
    cast(gl.createdbyloginid as varchar(5000)) as src_created_by_login_id,
    cast(gl.modifiedby as varchar(5000)) as src_modified_by,
    cast(gl.modifiedbyloginid as varchar(5000)) as src_modified_by_login_id,
    gl.accountno as account_no ,
    gl.accounttype as account_type,
    gl.alternativeaccount as account_no_alternative,
    nullif(gl.category,'') as category,
    gl.closingaccountno as account_no_closing,
    nullif(gl.closingaccounttitle,'') as closing_account_title,
    gl.closingtype as closing_type,
    ifnull(gl.enable_glmatching,false) as bln_enable_gl_matching,
    gl.normalbalance as normal_balance,
    ifnull(gl.requireclass,false) as bln_require_class,
    ifnull(gl.requirecustomer,false) as bln_require_customer,
    ifnull(gl.requiredept,false) as bln_require_department,
    ifnull(gl.requireemployee,false) as bln_require_employee,
    ifnull(gl.requiregldimline_tax_detail,false) as bln_require_gl_line_tax_detail,
    ifnull(gl.requiregldimvat_code,false)  as bln_require_gl_vat_code,
    ifnull(gl.requireitem,false) as bln_require_item,
    ifnull(gl.requireloc,false) as bln_require_location,
    ifnull(gl.requireproject,false) as bln_require_project,
    ifnull(gl.requiretask,false) as bln_require_task,
    ifnull(gl.requirevendor,false) as bln_require_vendor,
    gl.status,
    gl.subledgercontrolon as bln_subledgerr_control_on,
    gl.taxable as bln_taxable,
    gl.title,
    gl.whencreated as dts_src_created,
    gl.whenmodified as dts_src_modified
from glaccounts gl