set nocount on;
declare @Today date = getdate();
declare @EOM date = eomonth(@today);
declare @EOM_Diff int = datediff(day,@today,@EOM);
Declare @Vendor table (BDT_Vendor varchar(255), Vendor varchar(255));

insert into @Vendor
SELECT DISTINCT BDT_VENDOR, Vendor
	FROM {BM TBL} A
	INNER JOIN {VM TBL} B
	ON A.VendorMasterID=B.ID
	WHERE BDT_Vendor IS NOT NULL;

with
	BMI_Unmapped
As
(
	select
		ID,
		BMI_ID,
		Max_Date As Invoice_Date,
		Max_Date,
		'Unmapped' Tag,
		Audit_Result,
		Rep,
		Comment,
		Edit_Date

	from {UM TBL}
),
	BMI_ZeroRev
As
(
	select
		ID * -1 As ID,
		BMI_ID,
		Invoice_Date,
		Max_Date,
		Tag,
		Audit_Result,
		Rep,
		Comment,
		Edit_Date

	from {ZR TBL}
),
	PCI_Unmapped
As
(
	select
		ID,
		PCI_ID,
		Max_Date As Invoice_Date,
		Max_Date,
		'Unmapped' As Tag,
		Audit_Result,
		Rep,
		Comment,
		Edit_Date
  
	from {PUM TBL}
),
	PCI_ZeroRev
As
(
	select
		ID * -1 As ID,
		PCI_ID,
		Invoice_Date,
		Max_Date,
		Tag,
		Audit_Result,
		Rep,
		Comment,
		Edit_Date
  
	from {PZR TBL}
),
	CNR_Raw
As
(
	select
		'BMI' As Source_TBL,
		isnull(BMI_Unmapped.BMI_ID, BMI_ZeroRev.BMI_ID) As Source_ID,
		isnull(BMI_Unmapped.Invoice_Date, BMI_ZeroRev.Invoice_Date) As Invoice_Date,
		isnull(BMI_Unmapped.Max_Date, BMI_ZeroRev.Max_Date) As Max_Date,
		isnull(BMI_ZeroRev.Tag, BMI_Unmapped.Tag) As Tag,
		isnull(BMI_Unmapped.Audit_Result, BMI_ZeroRev.Audit_Result) As Audit_Result,
		isnull(BMI_Unmapped.Rep, BMI_ZeroRev.Rep) As Rep,
		isnull(BMI_Unmapped.Comment, BMI_ZeroRev.Comment) As Comment,
		isnull(BMI_Unmapped.Edit_Date, BMI_ZeroRev.Edit_Date) As Edit_Date

	from BMI_Unmapped
	full outer join BMI_ZeroRev
	on
		BMI_Unmapped.ID = BMI_ZeroRev.ID
	union all
	select
		'PCI' As Source_TBL,
		isnull(PCI_Unmapped.PCI_ID, PCI_ZeroRev.PCI_ID) As Source_ID,
		isnull(PCI_Unmapped.Invoice_Date, PCI_ZeroRev.Invoice_Date) As Invoice_Date,
		isnull(PCI_Unmapped.Max_Date, PCI_ZeroRev.Max_Date) As Max_Date,
		isnull(PCI_ZeroRev.Tag, PCI_Unmapped.Tag) As Tag,
		isnull(PCI_Unmapped.Audit_Result, PCI_ZeroRev.Audit_Result) As Audit_Result,
		isnull(PCI_Unmapped.Rep, PCI_ZeroRev.Rep) As Rep,
		isnull(PCI_Unmapped.Comment, PCI_ZeroRev.Comment) As Comment,
		isnull(PCI_Unmapped.Edit_Date, PCI_ZeroRev.Edit_Date) As Edit_Date

	from PCI_Unmapped
	full outer join PCI_ZeroRev
	on
		PCI_Unmapped.ID = PCI_ZeroRev.ID
),
	MY_TMP
As
(
	select
		Source_Tbl,
		Source_ID,
		Edit_Date,
		lag(audit_result)
				over(partition by Source_TBL, Source_ID order by edit_date) Previous_Result,
		iif
		(
			lag(audit_result)
				over(partition by Source_TBL, Source_ID order by edit_date)
				=
			audit_result,
			NULL,
			1
		) Indicator

	from CNR_Raw
),
	MY_TMP2
As
(
	select
		Source_TBL,
		Source_ID,
		Previous_Result,
		datediff(day,Edit_Date,@today) Time_Elapsed,
		row_number()
			over(partition by Source_TBL, Source_ID order by Edit_Date desc) Filter

	from MY_TMP

	where
		indicator=1
),
	Prev_Result
As
(
	Select
		Source_TBL,
		Source_ID,
		Previous_Result,
		Time_Elapsed

	from MY_TMP2

	where
		filter=1
),
	Max_Status
As
(
	select
		*,
		row_number()
			over(partition by source_tbl, source_id order by edit_date desc) Filter
	from CNR_Raw
),
	PRE_CNR
As
(
	select
		Time_Elapsed,
		A.Source_TBL,
		A.Source_ID,
		isnull(C.Vendor,D.Vendor) Vendor,
		isnull(C.BAN,D.BAN) BAN,
		isnull(C.BTN,D.BTN) BTN,
		isnull(C.WTN,D.WTN) WTN,
		isnull(C.Circuit_ID,D.Circuit_ID) Circuit_ID,
		MRC_Amount,
		isnull(C.Start_Date,D.Start_Date) Cost_Start_Date,
		isnull(C.Max_Date,D.Max_Date) Max_Cost_Date,
		isnull(C.End_Date, D.End_Date) Cost_End_Date,
		A.Invoice_Date Max_CNR_Date,
		case
			when G.Tag is not null then 'T'
			when Time_Elapsed is null then NULL
			when F.Low_Urgency is null then A.Audit_Result
			when MRC_Amount < 0 then 'Negative MRC Amount'
			when MRC_Amount = 0 then 'Zero MRC Amount'
			when Time_Elapsed < Low_Urgency then 'Low Urgency'
			when Time_Elapsed > High_Urgency then 'High Urgency'
			else
				'Moderate Urgency'
		end Priority,
		A.Tag,
		Previous_Result,
		A.Audit_Result,
		Rep,
		Comment,
		A.Edit_Date

	from Max_Status A
	left join Prev_Result B
	on A.Source_Tbl=B.Source_Tbl and A.Source_ID=B.Source_ID
	left join {BMI TBL} C
	on A.Source_ID=C.BMI_ID and A.Source_TBL='BMI'
	left join {PCI TBL} D
	on A.Source_ID=D.ID and A.Source_TBL='PCI'
	left join {CMRC TBL} E
	on A.Source_Tbl=E.Source_Tbl and A.Source_ID=E.Source_ID
	left join {AU TBL} F
	on A.Audit_Result=F.Audit_Result
	left join {T TBL} G
	on A.Source_ID=G.ID

	where
		filter=1
),
	CNR
As
(
	select
		CNR.Time_Elapsed,
		CNR.Source_TBL,
		CNR.Source_ID,
		isnull(Vendor.Vendor, CNR.Vendor) Vendor,
		CNR.BAN,
		CNR.BTN,
		CNR.WTN,
		CNR.Circuit_ID,
		CNR.MRC_Amount,
		CNR.Cost_Start_Date,
		CNR.Max_Cost_Date,
		CNR.Cost_End_Date,
		CNR.Max_CNR_Date,
		CNR.Priority,
		CNR.Tag,
		CNR.Previous_Result,
		CNR.Audit_Result,
		CNR.Rep,
		CNR.Comment,
		CNR.Edit_Date

	from PRE_CNR CNR
	left join @Vendor Vendor
	on
		CNR.Vendor = Vendor.BDT_Vendor
)

select
	*

from CNR

where
	Audit_Result NOT IN ('Billing Added', 'Disconnected', 'Mapped', 'Changed Mapping', 'Timing')
