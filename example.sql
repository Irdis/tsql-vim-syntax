GO
USE [AtlasCore]
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

/*
exec GetOpenTradeEntities 'HTFS','2014-06-27','OVR','OT',NULL,NULL,NULL,NULL,'HOUSE'
exec GetOpenTradeEntities 'HTFS','2014-04-28','COL','CPH',NULL,NULL,'HOUSE'

exec GetOpenTradeEntities 'ROL','2019-05-07','OVR','OT',NULL,NULL,NULL,NULL,'HOUSE'
exec GetOpenTradeEntities_old 'ROL','2019-05-07','OVR','OT',NULL,NULL,'HOUSE'
*/

CREATE OR ALTER PROCEDURE [dbo].[GetOpenTradeEntities]
    @CompanyCode            VARCHAR(100) = NULL,
    @FromDate               DATETIME = NULL,
    @PositionTypeCode       VARCHAR(20) = NULL,
    @CMSOverrideTypeCode    VARCHAR(50) = NULL,
    @PortfolioId            INT = NULL,
    @CustodianAccountId     INT = NULL,
    @CustodianAccountIds    VARCHAR(MAX) = NULL, -- #34379
    @PositionIds            VARCHAR(MAX) = NULL, -- #34377
    @VenderCode             VARCHAR(50) = NULL,
    @RemoveExcluded         BIT = NULL,
    @AgreementId            INT  = NULL,
    @PortfolioList          VARCHAR(MAX) = NULL,
    @PortfolioIds           VARCHAR(MAX) = NULL,
    @ShowInactiveAcc        BIT = NULL,             --#25792 NULL/0 don't show inactive accounts, 1 show
    @OutputTable            VARCHAR(128) = NULL,
	@ActiveAgreementsOnly	BIT = 0,
	@DataServiceRequiredColumns varchar(max) = NULL
/*
    COPYRIGHT: HazelTree ltd.
    Description:
        Datasource for screen Collateral \ OpenTrades
        #30487 new ver
    Authors:
        Mazhorin 2019-09-19 / 2020-05-29
*/
AS
BEGIN
--<trace>	HTFSLog.dbo.SPTraceGetParam @Db = 'AtlasCore', @ProcName = 'GetOpenTradeEntities'
	DECLARE @SPDb varchar(512), @SPName varchar(512), @SPPrms varchar(max), @SPInfo varchar(max), @SPTraceId int, @SPCnt int
	IF OBJECT_ID('tempdb.dbo.#trace') IS NULL CREATE TABLE #trace(Id int Primary Key NOT NULL)
	SELECT @SPDb = DB_NAME(), @SPName = OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID), @SPPrms =
		  '@CompanyCode='+ISNULL(''''+@CompanyCode+'''','NULL')+','
	    + '@FromDate='+ISNULL(''''+REPLACE(CONVERT(varchar(19),@FromDate,120),'-','')+'''','NULL')+','
	    + '@PositionTypeCode='+ISNULL(''''+@PositionTypeCode+'''','NULL')+','
	    + '@CMSOverrideTypeCode='+ISNULL(''''+@CMSOverrideTypeCode+'''','NULL')+','
	    + '@PortfolioId='+ISNULL(LTRIM(STR(@PortfolioId,10,0)),'NULL')+','
	    + '@CustodianAccountId='+ISNULL(LTRIM(STR(@CustodianAccountId,10,0)),'NULL')+','
	    + '@CustodianAccountIds='+ISNULL(''''+@CustodianAccountIds+'''','NULL')+','
	    + '@PositionIds='+ISNULL(''''+@PositionIds+'''','NULL')+','
	    + '@VenderCode='+ISNULL(''''+@VenderCode+'''','NULL')+','
	    + '@RemoveExcluded='+ISNULL(LTRIM(STR(@RemoveExcluded,4,0)),'NULL')+','
	    + '@AgreementId='+ISNULL(LTRIM(STR(@AgreementId,10,0)),'NULL')+','
	    + '@PortfolioList='+ISNULL(''''+@PortfolioList+'''','NULL')+','
	    + '@PortfolioIds='+ISNULL(''''+@PortfolioIds+'''','NULL')+','
	    + '@ShowInactiveAcc='+ISNULL(LTRIM(STR(@ShowInactiveAcc,4,0)),'NULL')+','
	    + '@OutputTable='+ISNULL(''''+@OutputTable+'''','NULL')+','
	    + '@ActiveAgreementsOnly='+ISNULL(LTRIM(STR(@ActiveAgreementsOnly,4,0)),'NULL')+','
	    + '@DataServiceRequiredColumns='+ISNULL(''''+@DataServiceRequiredColumns+'''','NULL')
	EXEC HTFSLog.dbo.SPTraceStart @SPDb = @SPDb, @SPName = @SPName, @SPPrms = @SPPrms, @SPTraceId = @SPTraceId OUTPUT
--</trace>

    PRINT REPLICATE('    ', @@NESTLEVEL - 1) + '----' + DB_NAME() + '.' + OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID) + '----'


    -- +++++++++++++++++++++ {__PROC_CALC_DECLARATIONS}
    SET NOCOUNT ON
	
	BEGIN TRY

    DECLARE
        -- common params
        @StartedAt          DATETIME2(7), -- small block moment
        @StartedAt2         DATETIME2(7) = SYSDATETIME(), -- large (whole) block moment
        @SubSPTraceId       INT,
        @IUDCount           INT = 0
    -- --------------------- {__PROC_CALC_DECLARATIONS}

	DECLARE @sql nvarchar(max)
    ---------------------------------------------
    -- adjust input parameters
    --
	DECLARE
        @OwnerCompanyId INT = dbo.GetOwnerCompanyId(),
        @CompanyId INT = dbo.GetCompanyIdByCode(@CompanyCode, default)

    IF @CompanyCode IS NULL
    BEGIN
        SET @CompanyCode = dbo.GetOwnerCompanyCode()
        SET @CompanyId = @OwnerCompanyId
    END
        
    DECLARE @CalendarId INT = (SELECT CalendarId FROM Company WHERE CompanyId = @CompanyId)
    IF @CalendarId IS NULL
    BEGIN
        SELECT @CalendarId = CalendarId FROM Company WHERE CompanyId = @OwnerCompanyId 
    END

    IF @FromDate IS NULL
    BEGIN
        SET @FromDate = dbo.GetPreviousBusinessDate(GETDATE(), @CalendarId)
    END
    
    IF @VenderCode IS NULL
    BEGIN
        SET @VenderCode = 'HOUSE'
    END

    IF @CMSOverrideTypeCode IS NULL 
    BEGIN
        SET @CMSOverrideTypeCode = 'OT'
    END

    IF @PositionTypeCode IS NULL OR @PositionTypeCode='OVR'
    BEGIN
        SET @PositionTypeCode = 'POS'
    END

    SET @RemoveExcluded = ISNULL(@RemoveExcluded, 0)

    -- list filters
    SET @CustodianAccountIds    = NULLIF(RTRIM(LTRIM(@CustodianAccountIds)), '')
    SET @PortfolioList          = NULLIF(RTRIM(LTRIM(@PortfolioList)), '')
    SET @PortfolioIds           = NULLIF(RTRIM(LTRIM(@PortfolioIds)), '')
    SET @PositionIds            = NULLIF(RTRIM(LTRIM(@PositionIds)), '')

    --
    -- adjust input parameters
    ---------------------------------------------

    DECLARE
        @FromDateId         INT = CONVERT(INT, CONVERT(CHAR(8), @FromDate, 112)),
        @CMSOverrideTypeId  INT = (SELECT CMSOverrideTypeId FROM dbo.CMSOverrideType WHERE Overridetypename = @CMSOverrideTypeCode),
        @PositionTypeId     INT = (SELECT PositionTypeId FROM dbo.PositionType WHERE PositionTypeCode = @PositionTypeCode),
        @PositionTypeId_POS INT = (SELECT PositionTypeId FROM dbo.PositionType WHERE PositionTypeCode = 'POS'),
        @CollateralModuleId INT = (SELECT EMTModuleId FROM dbo.EMTModule WHERE ModuleName = 'Collateral'),
        @IsHouse            BIT = (CASE WHEN @VenderCode = 'HOUSE' THEN 1 ELSE 0 END)

	DECLARE 
		@FromDatePrior		date = dbo.GetPreviousBusinessDate(@FromDate, @CalendarId)

	EXEC HTFSLog.dbo.SPTraceStart @SPDb = @SPDb, @SPName = @SPName, @SPTraceId = @SubSPTraceId OUTPUT, @SPSub = ' Init Required Columns ' 

	CREATE TABLE #Columns(Ord int identity(1,1), DisplayColumnName sysname primary key, ColumnExpression nvarchar(1024))
	CREATE TABLE #CheckColumns(DisplayColumnName sysname primary key)
	DECLARE @ExistRequiredColumns bit = 0 

	IF OBJECT_ID('tempdb..#GetOpenTradeEntities') IS NULL
	BEGIN
		IF @DataServiceRequiredColumns IS NOT NULL
		BEGIN
			INSERT INTO #CheckColumns(DisplayColumnName)
			SELECT DISTINCT ltrim(rtrim(tt.value))
			FROM string_split(@DataServiceRequiredColumns, ',') tt

			SET @ExistRequiredColumns = 1
		END
	END
	ELSE
	BEGIN
		IF OBJECT_ID('tempdb..#GetOpenTradeEntities') IS NOT NULL
		BEGIN
			INSERT INTO #CheckColumns(DisplayColumnName)
			SELECT c.name 
			FROM tempdb.sys.columns c
			WHERE c.object_id = OBJECT_ID('tempdb..#GetOpenTradeEntities')

			SET @ExistRequiredColumns = 1
		END
	END

	INSERT INTO #Columns(DisplayColumnName, ColumnExpression)
	SELECT t.DisplayColumnName, t.ColumnExpression
	FROM
	(
		VALUES
			 ('CMSOverrideID' ,  'ISNULL(od.CMSOverrideId, 0)')
			,('CMSOverridetypeid' ,  'ISNULL(od.CMSOverrideTypeId, 0)')
			,('OverrideDateid' ,  'ISNULL(od.CMSOverrideDateID, 0)')
			,('Positionid' ,  '#p.PositionId')
			,('CustodianAccountID' ,  'ca.CustodianAccountId')
			,('CustodianID' ,  'ca.CustodianId')
			,('Date' ,  'ISNULL(od.EffectiveDate, @FromDate)')
			,('StartDate' ,  'CONVERT(DATE, CONVERT(CHAR(8), NULLIF(p.StartDateId, 0)))')	
			,('EndDate' ,  'CONVERT(DATE, CONVERT(CHAR(8), NULLIF(p.EndDateId, 0)))')	
			,('Exclude' ,  'CAST(ISNULL(od.Exclude, 0) as bit)')
			,('OverrideOperation' ,  'ISNULL(od.Overrideoperation, '''')')
			,('PortfolioID' ,  'ca.PortfolioId')
			,('Price' ,  'ISNULL(od.PriceLocal, pga_h.PriceLocal)')
			,('Securityid' ,  's.SecurityId')
			--,('SettlementDate' ,  'ISNULL(od.SettlementDate, pca_h.SettlementDate)')
			,('PositionDirection' ,  'CAST(ISNULL(od.TransactionType, p.PositionDirection) as char(1))')	
			,('CustodianAccountCode' ,  'ca.CustodianAccountCode')
			,('CustodianCode' ,  'cust.CustodianCode')
			,('CustodianName' ,  'cust.CustodianName')
			,('portfolioCode' ,  'po.PortfolioCode')
			,('portfolioName' ,  'po.PortfolioName')
            ,('SecurityCompanyCode' ,  'seccompany.CompanyCode')
			,('SecurityCode' ,  's.SecurityCode')
			,('UnderlyerSecurityCode' ,  'su.SecurityCode')
			,('CurrencyCode' ,  'ccy.CurrencyCode')
			,('Currencyid' ,  'ccy.CurrencyId')
			,('SecurityIssuerCode' ,  'si.SecurityIssuerCode')
			,('NaturalKey' ,  'pca_h.NaturalKey')
			,('positiondateid' ,  'ISNULL(od.CMSOverrideDateId, pd.PositionDateId)')
			,('FXRate' ,  'CAST(COALESCE(od.FXRate, pca_h.FxRate2Agreement, 1) as decimal(28, 18))')
			,('SecurityTypeId' ,  's.SecurityTypeId')
			,('SecurityTypeCode' ,  'st.SecurityTypeCode')
			,('SecurityDesc' ,  's.SecurityDesc')
			,('OMSTicker' ,  'public_ids.OMSTicker')
			,('PricingFactor' ,  'ISNULL(od.PricingFactor, pca_h.PricingFactor)')
			,('ContractSize' ,  'ISNULL(od.ContractSize, pca_h.ContractSize)')
			,('AgreementTypeCode' ,  'ad.AgreementTypeCode')
			,('IMOverridePercent' ,  'CAST(COALESCE(od.IMOverridePercent, pca_h.IMOverridePercent, pca_h.IAPct) as decimal(28, 10))')
			,('CsaStatus' ,  'ad.CSAStatus')
			,('AgreementId' ,  'ad.AgreementId')
			
			,('TradingStrategyId' ,  'p.TradingStrategyId')
			,('IsMarginMTMBased' ,  'ISNULL(st.IsMarginMTMBased, 0)')
			,('PositionType' ,  '@PositionTypeCode')
			,('AvgCostLocal' ,  'od.AvgCostLocal')
			,('TradeId' ,  'isnull(pca_h.TradeId, 0)')
			,('TradingStrategyCode' ,  'pts.TradingStrategyCode')
			,('AccruedInterest' ,  'CAST(0 as decimal(28, 8))')
			,('AccountTypeCode' ,  'at.AccountTypeCode')
			
			,('CallSubTypeId' ,  'ISNULL(od.CallSubTypeId, pca_h.CallSubTypeId)')
			,('CallSubTypeName' ,  'cst.CallSubTypeName')
			,('FCMFeesLocalFund' ,  'pca_h.FCMFeesLocal')
			,('FCMFeesBaseFund' ,  'CAST(ISNULL(pca_h.FCMFeesBase * pca_h.FxRatePortfolio2Agreement, pca_h.FCMFeesBase) as decimal(28, 8))')
			,('FCMSettlementLocalFund' ,  'pca_h.FCMSettlementLocal')
			,('FCMSettlementBaseFund' ,  'CAST(ISNULL(pca_h.FCMSettlementBase * pca_h.FxRatePortfolio2Agreement, pca_h.FCMSettlementBase) as decimal(28, 8))')
			,('FCMFeesLocalBroker' ,  'pca_b.FCMFeesLocal')
			,('FCMFeesBaseBroker' ,  'pca_b.FCMFeesBase')
			,('FCMSettlementLocalBroker' ,  'pca_b.FCMSettlementLocal')
			,('FCMSettlementBaseBroker' ,  'pca_b.FCMSettlementBase')
			,('PAILocalFund' ,  'pca_h.PAILocal')
			,('PAIBaseFund' ,  'CAST(ISNULL(pca_h.PAIBase * pca_h.FxRatePortfolio2Agreement, pca_h.PAIBase) as decimal(28, 8))')
			,('PAIBaseBroker' ,  'pca_b.PAIBase')
			,('RollupCustodianAccountId' ,  'ca.RollupCustodianAccountId')
			,('RollupCustodianAccountCode' ,  'ISNULL(ca_roll.CustodianAccountCode, ca.CustodianAccountCode)')
			,('NegotiatedIALocal' ,  'pca_h.NegotiatedIALocal')
			,('NegotiatedIABase' ,  'CAST(ISNULL(pca_h.NegotiatedIABase * pca_h.FxRatePortfolio2Agreement, pca_h.NegotiatedIABase) as decimal(28, 8))')
			,('NegotiatedIAPercent' ,  'pca_h.NegotiatedIAPercent')
			,('AccruedInterestBase' ,  'pca_h.AccruedInterestBase')
			,('AccruedInterestLocal' ,  'pca_h.AccruedInterestLocal')
			
			,('UnderlyingCleanPrice' ,  'pca_h.UnderlyingCleanPrice')
			,('UnderlyingCouponAccrual' ,  'CAST(pca_h.UnderlyingDirtyPrice - pca_h.UnderlyingCleanPrice as decimal(28, 8))')
			,('UnderlyingCUSIP' ,  'su.CUSIP')
			,('UnderlyingDirtyPrice' ,  'pca_h.UnderlyingDirtyPrice')
			,('UnderlyingISIN' ,  'su.ISIN')
			,('UnderlyingMarketValBaseTD' ,  'pca_h.UnderlyingMarketValBaseTD')
			,('UnderlyingMarketValLocalTD' ,  'pca_h.UnderlyingMarketValLocalTD')
			,('UnderlyingMaturityDate' ,  'CAST(sfisu.MaturityDate as date)')
			,('UnderlyingSecurityType' ,  'stu.SecurityTypeCode')
			,('AssetId' ,  'ISNULL(su.SecurityId, s.SecurityId)')
			,('Asset' ,  'ISNULL(su.SecurityCode, s.SecurityCode)')
			,('CUSIP' ,  'public_ids.Cusip')
			,('SEDOL' ,  'public_ids.Sedol')
			,('ISIN' ,  'public_ids.ISIN')
			,('BbergCode' ,  'public_ids.BbergCode')
			,('ReutersCode' ,  'public_ids.ReutersCode')
			,('CountryCode' ,  'ISNULL(c.CountryCode, cu.CountryCode)')
			,('IsStub' ,  's.IsStub')
			
			,('PortfolioFocus', 'pa.PortfolioFocus') 
			,('PortfolioManager', 'pa.PortfolioManager') 
			,('PortfolioStyle', 'pa.PortfolioStyle') 
			,('PortfolioTheme', 'pa.PortfolioTheme') 
			,('IsRolloverAccruedInterestLocal'						,  'ISNULL(pcas_h.AccruedInterestLocal, 0)')
			,('IsRolloverAccruedInterestBase'						,  'ISNULL(pcas_h.AccruedInterestBase, 0)')
			,('IsRolloverCounterpartyInitialMarginBase'				,  'ISNULL(pcas_b.InitialMarginBase, 0)')
			,('IsRolloverCounterpartyInitialMarginLocal'			,  'ISNULL(pcas_b.InitialMarginLocal, 0)')
			,('IsRolloverCounterpartyNotionalBase'					,  'ISNULL(pgas_b.NotionalBase, 0)')
			,('IsRolloverCounterpartyNotionalLocal'					,  'ISNULL(pgas_b.NotionalLocal, 0)')
			,('IsRolloverFCMFeesBaseBroker'							,  'ISNULL(pcas_b.FCMFeesBase, 0)')
			,('IsRolloverFCMFeesBaseFund'							,  'ISNULL(pcas_h.FCMFeesBase, 0)')
			,('IsRolloverFCMFeesLocalBroker'						,  'ISNULL(pcas_b.FCMFeesLocal, 0)')
			,('IsRolloverFCMFeesLocalFund'							,  'ISNULL(pcas_h.FCMFeesLocal, 0)')
			,('IsRolloverFCMSettlementBaseBroker'					,  'ISNULL(pcas_b.FCMSettlementBase, 0)')
			,('IsRolloverFCMSettlementBaseFund'						,  'ISNULL(pcas_h.FCMSettlementBase, 0)')
			,('IsRolloverFCMSettlementLocalBroker'					,  'ISNULL(pcas_b.FCMSettlementLocal, 0)')
			,('IsRolloverFCMSettlementLocalFund'					,  'ISNULL(pcas_h.FCMSettlementLocal, 0)')
			,('IsRolloverFXRate'									,  'ISNULL(pcas_h.FxRate2Agreement, 0)')
			,('IsRolloverInternalHaircutPct'						,  'ISNULL(pcas_h.HaircutPct, 0)')
			,('IsRolloverCounterpartyHaircutPct'					,  'ISNULL(pcas_b.HaircutPct, 0)')
			
			,('IsRolloverInternalIAPct'								,  'ISNULL(pcas_h.IAPct, 0)')
			,('IsRolloverCounterpartyIAPct'							,  'ISNULL(pcas_b.IAPct, 0)')
			,('IsRolloverInternalInitialMarginBase'					,  'CAST(ISNULL(IIF(od.InitialMarginBase IS NOT NULL, 0, pcas_h.InitialMarginBase), 0) as bit)')
			,('IsRolloverInternalInitialMarginLocal'				,  'CAST(ISNULL(IIF(od.InitialMarginLocal IS NOT NULL, 0, pcas_h.InitialMarginLocal), 0) as bit)')
			,('IsRolloverInternalLoanAmountBase'					,  'ISNULL(pcas_h.LoanAmountBase, 0)')
			,('IsRolloverInternalLoanAmountLocal'					,  'ISNULL(pcas_h.LoanAmountLocal, 0)')
			,('IsRolloverCounterpartyLoanAmountBase'				,  'ISNULL(pcas_b.LoanAmountBase, 0)')
			,('IsRolloverCounterpartyLoanAmountLocal'				,  'ISNULL(pcas_b.LoanAmountLocal, 0)')
			,('IsRolloverInternalLoanAmountPlusRepoAccruedInterestBase' ,  'ISNULL(pcas_h.LoanAmountPlusRepoAccruedInterestBase, 0)')
			,('IsRolloverInternalLoanAmountPlusRepoAccruedInterestLocal',  'ISNULL(pcas_h.LoanAmountPlusRepoAccruedInterestLocal, 0)')
			,('IsRolloverCounterpartyLoanAmountPlusRepoAccruedInterestBase' ,  'ISNULL(pcas_b.LoanAmountPlusRepoAccruedInterestBase, 0)')
			,('IsRolloverCounterpartyLoanAmountPlusRepoAccruedInterestLocal',  'ISNULL(pcas_b.LoanAmountPlusRepoAccruedInterestLocal, 0)')
			,('IsRolloverInternalMTMBase'							,  'CAST(ISNULL(IIF(od.MTMBase IS NOT NULL, 0, pgas_h.MTMBase), 0) as bit)')
			,('IsRolloverInternalMTMLocal'							,  'CAST(ISNULL(IIF(od.MTMLocal IS NOT NULL, 0, pgas_h.MTMLocal), 0) as bit)')
			,('IsRolloverCounterpartyMTMBase'						,  'CAST(ISNULL(pgas_b.MTMBase, 0) as bit)')
			,('IsRolloverCounterpartyMTMLocal'						,  'CAST(ISNULL(pgas_b.MTMLocal, 0) as bit)')
			,('IsRolloverInternalNotionalAmountBase'				,  'CAST(ISNULL(IIF(od.NotionalBase IS NOT NULL, 0, pgas_h.NotionalBase), 0) as bit)')
			,('IsRolloverInternalNotionalAmountLocal'				,  'CAST(ISNULL(IIF(od.NotionalLocal IS NOT NULL, 0, pgas_h.NotionalLocal), 0) as bit)')
			,('IsRolloverCounterpartyNotionalAmountBase'			,  'CAST(ISNULL(pgas_b.NotionalBase, 0) as bit)')
			,('IsRolloverCounterpartyNotionalAmountLocal'			,  'CAST(ISNULL(pgas_b.NotionalLocal, 0) as bit)')
			,('IsRolloverPAIBaseFund'								,  'ISNULL(pcas_h.PAIBase, 0)')
			,('IsRolloverPAILocalFund'								,  'ISNULL(pcas_h.PAILocal, 0)')
			,('IsRolloverInternalPrincipalFactor'					,  'ISNULL(pcas_h.PrincipalFactor, 0)')
			,('IsRolloverCounterpartyPrincipalFactor'				,  'ISNULL(pcas_b.PrincipalFactor, 0)')
			,('IsRolloverInternalQuantityTD'						,  'ISNULL(pgas_h.QuantityTD, 0)')
			,('IsRolloverInternalRepoAccruedInterestLocal'			,  'ISNULL(pcas_h.RepoAccruedInterestLocal, 0)')
			,('IsRolloverInternalRepoAccruedInterestBase'			,  'ISNULL(pcas_h.RepoAccruedInterestBase, 0)')
			,('IsRolloverCounterpartyRepoAccruedInterestLocal'		,  'ISNULL(pcas_b.RepoAccruedInterestLocal, 0)')
			,('IsRolloverCounterpartyRepoAccruedInterestBase'		,  'ISNULL(pcas_b.RepoAccruedInterestBase, 0)')
			,('IsRolloverInternalRepoRate'							,  'ISNULL(pcas_h.RepoRate, 0)')
			,('IsRolloverCounterpartyRepoRate'						,  'ISNULL(pcas_b.RepoRate, 0)')
			,('IsRolloverInternalSecondNotional'					,  'ISNULL(pcas_h.SecondNotional, 0)')
			,('IsRolloverCounterpartySecondNotional'				,  'ISNULL(pcas_b.SecondNotional, 0)')
			,('IsRolloverUnderlyingCleanPrice'						,  'ISNULL(pcas_h.UnderlyingCleanPrice, 0)')
			,('IsRolloverUnderlyingDirtyPrice'						,  'ISNULL(pcas_h.UnderlyingDirtyPrice, 0)')
			,('IsRolloverUnderlyingMarketValLocalTD'				,  'ISNULL(pcas_h.UnderlyingMarketValLocalTD, 0)')
			,('IsRolloverUnderlyingMarketValBaseTD'					,  'ISNULL(pcas_h.UnderlyingMarketValBaseTD, 0)')
			,('IsRolloverCounterpartyQuantityTD'					,  'ISNULL(pgas_b.QuantityTD, 0)')
			,('IsRolloverPAIBaseBroker'								,  'ISNULL(pcas_b.PAIBase, 0)')

	-- "Internal Data" band
			,('InternalIAPct'										,  'CAST(Internal.IAPct as decimal(28, 8))')

			,('InternalSecCode'										,  'prd.ClientSecCode')
			,('InternalAccount'										,  'prd.ClientAccount')
			,('InternalPositionId'									,  'prd.ClientPositionId')
			,('InternalSecurityId'									,  'prd.ClientSecurityId')
			,('InternalSecType'										,  'prd.ClientSecType')
			,('InternalSecurityDesc'								,  'prd.ClientSecurityDesc')

			,('InternalNotionalAmountBase'							,  'CAST(Internal.NotionalAmountBase as decimal(28, 8))')
			,('InternalNotionalAmountLocal'							,  'CAST(Internal.NotionalAmountLocal as decimal(28, 8))')
			,('InternalMTMBase'										,  'CAST(Internal.MTMBase as decimal(28, 8))')
			,('InternalMTMLocal'									,  'Internal.MTMLocal')
			,('InternalInitialMarginBase'							,  'CAST(Internal.InitialMarginBase as decimal(28, 8))')
			,('InternalInitialMarginLocal'							,  'Internal.InitialMarginLocal')
			,('InternalEffectiveDate'								,  'COALESCE(od.EffectiveDate, pga_h.EffectiveDate, sfis.EffectiveDate, @FromDate)')
			,('InternalSecondNotional'								,  'pca_h.SecondNotional')
			,('InternalMaturityDate'								,  'ISNULL(od.MaturityDate, pca_h.MaturityDate)')
			,('InternalHaircutPct'									,  'pca_h.HaircutPct')
			,('InternalLoanAmountBase'								,  'pca_h.LoanAmountBase')
			,('InternalLoanAmountLocal'								,  'pca_h.LoanAmountLocal')
			,('InternalLoanAmountPlusRepoAccruedInterestBase'		,  'pca_h.LoanAmountPlusRepoAccruedInterestBase')
			,('InternalLoanAmountPlusRepoAccruedInterestLocal'		,  'pca_h.LoanAmountPlusRepoAccruedInterestLocal')
			,('InternalPrincipalFactor'								,  'ISNULL(pca_h.PrincipalFactor, sfis.CurrentFactor)')
			,('InternalRepoAccruedInterestBase'						,  'pca_h.RepoAccruedInterestBase')
			,('InternalRepoAccruedInterestLocal'					,  'pca_h.RepoAccruedInterestLocal')
			,('InternalRepoRate'									,  'pca_h.RepoRate')
			,('InternalStrikePriceLocal'							,  'ISNULL(od.StrikePriceLocal, pca_h.StrikePriceLocal)')
			,('InternalQuantityTD'									,  'pga_h.QuantityTD')

			,('InternalBuyCurrency'									,  'pca_h.BuyCurrency')
			,('InternalSoldCurrency'								,  'pca_h.SoldCurrency')	
			,('InternalBoughtAmount'								,  'pca_h.BoughtAmount')	
			,('InternalSoldAmount'									,  'pca_h.SoldAmount')
			
			-- prior columns
			,('InternalPriorMTMBase'								,  'CAST(Internal.PriorMTMBase as decimal(28, 8))')
			,('InternalPriorMTMLocal'								,  'Internal.PriorMTMLocal')
			,('InternalPriorInitialMarginBase'						,  'CAST(Internal.PriorInitialMarginBase as decimal(28, 8))')
			,('InternalPriorInitialMarginLocal'						,  'Internal.PriorInitialMarginLocal')

			-- diff columns
			,('InternalPriorMTMBaseDiff'							,  'CAST(CASE WHEN COALESCE(Internal.MTMBase, Internal.PriorMTMBase) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Internal.MTMBase, 0)) - ABS(ISNULL(Internal.PriorMTMBase, 0))) END as decimal(28, 8))')
			,('InternalPriorMTMLocalDiff'							,  'CAST(CASE WHEN COALESCE(Internal.MTMLocal, Internal.PriorMTMLocal) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Internal.MTMLocal, 0)) - ABS(ISNULL(Internal.PriorMTMLocal, 0))) END as decimal(28, 8))')
			,('InternalPriorInitialMarginBaseDiff'					,  'CAST(CASE WHEN COALESCE(Internal.InitialMarginBase, Internal.PriorInitialMarginBase) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Internal.InitialMarginBase, 0)) - ABS(ISNULL(Internal.PriorInitialMarginBase, 0))) END as decimal(28, 8))')
			,('InternalPriorInitialMarginLocalDiff'					,  'CAST(CASE WHEN COALESCE(Internal.InitialMarginLocal, Internal.PriorInitialMarginLocal) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Internal.InitialMarginLocal, 0)) - ABS(ISNULL(Internal.PriorInitialMarginLocal, 0))) END as decimal(28, 8))')

	-- "Counterparty Data" band
			,('CounterpartyIAPct'									,  'CAST(Counterparty.IAPct as decimal(28, 8))')
			
			,('CounterpartySecCode'									,  'prd.BrokerSecCode')
			,('CounterpartyAccount'									,  'prd.BrokerAccount')
			,('CounterpartySecurityId'								,  'prd.BrokerSecurityId')
			,('CounterpartySecType'									,  'prd.BrokerSecType')
			,('CounterpartySecurityDesc'							,  'prd.BrokerSecurityDesc')
			
			,('CounterpartyNotionalAmountBase'						,  'CAST(Counterparty.NotionalAmountBase as decimal(28, 8))')
			,('CounterpartyNotionalAmountLocal'						,  'CAST(Counterparty.NotionalAmountLocal as decimal(28, 8))')
			,('CounterpartyMTMBase'									,  'CAST(Counterparty.MTMBase as decimal(28, 8))')
			,('CounterpartyMTMLocal'								,  'Counterparty.MTMLocal')
			,('CounterpartyInitialMarginBase'						,  'CAST(Counterparty.InitialMarginBase as decimal(28, 8))')
			,('CounterpartyInitialMarginLocal'						,  'Counterparty.InitialMarginLocal')
			,('CounterpartyEffectiveDate'							,  'pga_b.EffectiveDate')
			,('CounterpartySecondNotional'							,  'pca_b.SecondNotional')
			,('CounterpartyMaturityDate'							,  'pca_b.MaturityDate')
			,('CounterpartyQuantityTD'								,  'pga_b.QuantityTD')

			,('CounterpartyNotionalBase'							,  'CAST(Counterparty.NotionalAmountBase as decimal(28, 8))')
			,('CounterpartyNotionalLocal'							,  'CAST(Counterparty.NotionalAmountLocal as decimal(28, 8))')

			-- prior columns
			,('CounterpartyPriorMTMBase'							,  'CAST(Counterparty.PriorMTMBase as decimal(28, 8))')
			,('CounterpartyPriorMTMLocal'							,  'Counterparty.PriorMTMLocal')
			,('CounterpartyPriorInitialMarginBase'					,  'CAST(Counterparty.PriorInitialMarginBase as decimal(28, 8))')
			,('CounterpartyPriorInitialMarginLocal'					,  'Counterparty.PriorInitialMarginLocal')

			-- diff columns
			,('CounterpartyPriorMTMBaseDiff'						,  'CAST(CASE WHEN COALESCE(Counterparty.MTMBase, Counterparty.PriorMTMBase) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Counterparty.MTMBase, 0)) - ABS(ISNULL(Counterparty.PriorMTMBase, 0))) END as decimal(28, 8))')
			,('CounterpartyPriorMTMLocalDiff'						,  'CAST(CASE WHEN COALESCE(Counterparty.MTMLocal, Counterparty.PriorMTMLocal) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Counterparty.MTMLocal, 0)) - ABS(ISNULL(Counterparty.PriorMTMLocal, 0))) END as decimal(28, 8))')
			,('CounterpartyPriorInitialMarginBaseDiff'				,  'CAST(CASE WHEN COALESCE(Counterparty.InitialMarginBase, Counterparty.PriorInitialMarginBase) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Counterparty.InitialMarginBase, 0)) - ABS(ISNULL(Counterparty.PriorInitialMarginBase, 0))) END as decimal(28, 8))')
			,('CounterpartyPriorInitialMarginLocalDiff'				,  'CAST(CASE WHEN COALESCE(Counterparty.InitialMarginLocal, Counterparty.PriorInitialMarginLocal) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Counterparty.InitialMarginLocal, 0)) - ABS(ISNULL(Counterparty.PriorInitialMarginLocal, 0))) END as decimal(28, 8))')

	-- "Repo Related" band
			,('CounterpartyHaircutPct'								,  'pca_b.HaircutPct')
			,('CounterpartyLoanAmountBase'							,  'pca_b.LoanAmountBase')
			,('CounterpartyLoanAmountLocal'							,  'pca_b.LoanAmountLocal')
			,('CounterpartyLoanAmountPlusRepoAccruedInterestBase'	,  'pca_b.LoanAmountPlusRepoAccruedInterestBase')
			,('CounterpartyLoanAmountPlusRepoAccruedInterestLocal'	,  'pca_b.LoanAmountPlusRepoAccruedInterestLocal')
			,('CounterpartyPrincipalFactor'							,  'pca_b.PrincipalFactor')
			,('CounterpartyRepoAccruedInterestBase'					,  'pca_b.RepoAccruedInterestBase')
			,('CounterpartyRepoAccruedInterestLocal'				,  'pca_b.RepoAccruedInterestLocal')
			,('CounterpartyRepoRate'								,  'pca_b.RepoRate')
			,('CounterpartyStrikePriceLocal'						,  'pca_b.StrikePriceLocal')
			,('UnderlyingCcy'										,  'ccyu.CurrencyCode')
			,('UnderlyingDesc'										,  'su.SecurityDesc')

			,('CounterpartyStartDate'								,  'prd.BrokerStartDate')
			,('CounterpartyEndDate'									,  'prd.BrokerEndDate')
			,('CounterpartyDirection'								,  'prd.BrokerDirection')
			,('CounterpartyUnderlyingSecCode'						,  'prd.BrokerUnderlyingSecurityCode')
			,('CounterpartyUnderlyingSecurityDesc'					,  'prd.BrokerUnderlyingSecurityDescription')

			,('CounterpartyBuyCurrency'								,  'pca_b.BuyCurrency')
			,('CounterpartySoldCurrency'							,  'pca_b.SoldCurrency')	
			,('CounterpartyBoughtAmount'							,  'pca_b.BoughtAmount')	
			,('CounterpartySoldAmount'								,  'pca_b.SoldAmount')

	-- Diffs
			,('QuantityTDDiff'										,  'CAST(CASE WHEN COALESCE(pga_h.QuantityTD, pga_b.QuantityTD) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(pga_h.QuantityTD, 0)) - ABS(ISNULL(pga_b.QuantityTD, 0))) END as decimal(28, 8))')
			,('MTMBaseDiff'											,  'CAST(CASE WHEN COALESCE(Internal.MTMBase, Counterparty.MTMBase) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Internal.MTMBase, 0)) - ABS(ISNULL(Counterparty.MTMBase, 0))) END as decimal(28, 8))')
			,('MTMLocalDiff'										,  'CAST(CASE WHEN COALESCE(Internal.MTMLocal, Counterparty.MTMLocal) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Internal.MTMLocal, 0)) - ABS(ISNULL(Counterparty.MTMLocal, 0))) END as decimal(28, 8))')
			,('IAPctDiff'											,  'CAST(CASE WHEN COALESCE(Internal.IAPct, Counterparty.IAPct) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Internal.IAPct, 0)) - ABS(ISNULL(Counterparty.IAPct, 0))) END as decimal(28, 8))')
			,('InitialMarginBaseDiff'								,  'CAST(CASE WHEN COALESCE(Internal.InitialMarginBase, Counterparty.InitialMarginBase) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Internal.InitialMarginBase, 0)) - ABS(ISNULL(Counterparty.InitialMarginBase, 0))) END as decimal(28, 8))')
			,('InitialMarginLocalDiff'								,  'CAST(CASE WHEN COALESCE(Internal.InitialMarginLocal, Counterparty.InitialMarginLocal) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Internal.InitialMarginLocal, 0)) - ABS(ISNULL(Counterparty.InitialMarginLocal, 0))) END as decimal(28, 8))')
			,('NotionalAmountBaseDiff'								,  'CAST(CASE WHEN COALESCE(Internal.NotionalAmountBase, Counterparty.NotionalAmountBase) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Internal.NotionalAmountBase, 0)) - ABS(ISNULL(Counterparty.NotionalAmountBase, 0))) END as decimal(28, 8))')
			,('NotionalAmountLocalDiff'								,  'CAST(CASE WHEN COALESCE(Internal.NotionalAmountLocal, Counterparty.NotionalAmountLocal) IS NULL THEN NULL ELSE ABS(ABS(ISNULL(Internal.NotionalAmountLocal, 0)) - ABS(ISNULL(Counterparty.NotionalAmountLocal, 0))) END as decimal(28, 8))')
	
			
	)t(DisplayColumnName, ColumnExpression)
	WHERE 
		(@ExistRequiredColumns = 0 OR EXISTS(SELECT 1 FROM #CheckColumns cc WHERE cc.DisplayColumnName = t.DisplayColumnName))
	SET @SPCnt = @@ROWCOUNT

	EXEC HTFSLog.dbo.SPTraceFinish @SPCnt = @SPCnt, @SPTraceId = @SubSPTraceId, @SPInfo = @SPInfo


    SET @StartedAt = SYSDATETIME(); SET @SPCnt = 0
    EXEC HTFSLog.dbo.SPTraceStart @SPDb = @SPDb, @SPName = @SPName, @SPTraceId = @SubSPTraceId OUTPUT, @SPSub = '#Portfolio'
    SELECT DISTINCT p.PortfolioId
    INTO #Portfolio
    FROM dbo.Portfolio p
        LEFT JOIN dbo.SplitStr2Table(@PortfolioList, ',', 1, 1) pl ON pl.s = p.PortfolioCode
        LEFT JOIN dbo.SplitStr2Table(@PortfolioIds, ',', 1, 1) pid ON pid.s = CONVERT(VARCHAR(10), p.PortfolioId)
    WHERE (@PortfolioId IS NULL OR p.PortfolioId = @PortfolioId)
        AND (@PortfolioList IS NULL OR pl.s IS NOT NULL)
        AND (@PortfolioIds IS NULL OR pid.s IS NOT NULL)
        AND p.CompanyID = @CompanyId
    SET @SPCnt = @@ROWCOUNT;
    CREATE UNIQUE CLUSTERED INDEX PK_#Portfolio ON #Portfolio (PortfolioId)
    SET @SPInfo =
        REPLICATE('    ', @@NESTLEVEL) +
        '#Portfolio: inserted ' + CONVERT(VARCHAR(11), @SPCnt) + ' rows, time spent ' +
        CONVERT(VARCHAR(22), DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, @StartedAt, SYSDATETIME()), '00:00:00' ), 114)
    PRINT @SPInfo;
    EXEC HTFSLog.dbo.SPTraceFinish @SPTraceId = @SubSPTraceId, @SPCnt = @SPCnt

    SET @StartedAt = SYSDATETIME(); SET @SPCnt = 0
    EXEC HTFSLog.dbo.SPTraceStart @SPDb = @SPDb, @SPName = @SPName, @SPTraceId = @SubSPTraceId OUTPUT, @SPSub = '#CustodianAccount'
    SELECT
        ca.CustodianAccountId,
        ca.RollupCustodianAccountId,
        AgreementId = CONVERT(INT, NULL),
		CustodianAccountId_Final = ISNULL(ca.RollupCustodianAccountId, ca.CustodianAccountId)
    INTO #CustodianAccount
    FROM dbo.CustodianAccount ca
        INNER JOIN #Portfolio p ON p.PortfolioId = ca.PortfolioId
        LEFT JOIN dbo.CustodianAccount rca ON rca.CustodianAccountId = ca.RollupCustodianAccountId
        LEFT JOIN (SELECT DISTINCT CustodianAccountId = CONVERT(INT, s) FROM dbo.SplitStr2Table(@CustodianAccountIds, ',', 1, 1)) #ca
            ON #ca.CustodianAccountId IN (ca.CustodianAccountId, ca.RollupCustodianAccountId)
    WHERE (@ShowInactiveAcc = 1 OR ca.IsActive = 1) -- #25792 Account is Active ( would like a check box ON top to enable showing inactive accounts - See Account Page)
        AND (ca.IsOTC = 1 OR rca.IsOTC = 1) -- #25792 (Account is flagged AS OTC) OR (Account is a child of an OTC account)
        AND (@CustodianAccountId IS NULL OR @CustodianAccountId IN (ca.CustodianAccountId, ca.RollupCustodianAccountId))
        AND (@CustodianAccountIds IS NULL OR #ca.CustodianAccountId IS NOT NULL)
    SET @SPCnt = @@ROWCOUNT;
    CREATE UNIQUE CLUSTERED INDEX PK_#CustodianAccount ON #CustodianAccount (CustodianAccountId)
    CREATE NONCLUSTERED INDEX IX_#CustodianAccount_CustodianAccountId_Final ON #CustodianAccount (CustodianAccountId_Final)
	SET @SPInfo =
        REPLICATE('    ', @@NESTLEVEL) +
        '#CustodianAccount: inserted ' + CONVERT(VARCHAR(11), @SPCnt) + ' rows, time spent ' +
        CONVERT(VARCHAR(22), DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, @StartedAt, SYSDATETIME()), '00:00:00' ), 114)
    PRINT @SPInfo;
    EXEC HTFSLog.dbo.SPTraceFinish @SPTraceId = @SubSPTraceId, @SPCnt = @SPCnt


    SET @StartedAt = SYSDATETIME(); SET @SPCnt = 0
    EXEC HTFSLog.dbo.SPTraceStart @SPDb = @SPDb, @SPName = @SPName, @SPTraceId = @SubSPTraceId OUTPUT, @SPSub = '#CustodianAccount'
    SELECT p.PositionId
    INTO #Position
    FROM dbo.Position p
        INNER JOIN (SELECT DISTINCT s FROM dbo.SplitStr2Table(@PositionIds, ',', 1, 1)) #p ON #p.s = CONVERT(VARCHAR(10), p.PositionId)
        INNER JOIN #CustodianAccount ca on ca.CustodianAccountId = p.CustodianAccountId -- should be applied even for @PositionIds<>NULL
    SET @SPCnt = @@ROWCOUNT;
    CREATE UNIQUE CLUSTERED INDEX PK_#Position ON #Position (PositionId)
    SET @SPInfo =
        REPLICATE('    ', @@NESTLEVEL) +
        '#Position: inserted ' + CONVERT(VARCHAR(11), @SPCnt) + ' rows, time spent ' +
        CONVERT(VARCHAR(22), DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, @StartedAt, SYSDATETIME()), '00:00:00' ), 114)
    PRINT @SPInfo;
    EXEC HTFSLog.dbo.SPTraceFinish @SPTraceId = @SubSPTraceId, @SPCnt = @SPCnt


    SET @StartedAt = SYSDATETIME(); SET @SPCnt = 0
    EXEC HTFSLog.dbo.SPTraceStart @SPDb = @SPDb, @SPName = @SPName, @SPTraceId = @SubSPTraceId OUTPUT, @SPSub = '#AgreementDetailData'
    SELECT TOP (0) AgreementId, OtcAccountId, IAPledgedAccountId, IASecuredAccountId, VMPledgedAccountId, VMSecuredAccountId, RegIMPledgedAccountId, RegIMSecuredAccountId, AgreementTypeCode, CSAStatus, IsActive 
	INTO #GetAgreementDetail 
	FROM template.udtGetAgreementDetail
    
	EXEC dbo.GetAgreementDetail 
		@CompanyCode = @CompanyCode, 
		@DataServiceRequiredColumns = 'AgreementId,OtcAccountId,IAPledgedAccountId,IASecuredAccountId,VMPledgedAccountId,VMSecuredAccountId,RegIMPledgedAccountId,RegIMSecuredAccountId,AgreementTypeCode,CSAStatus,IsActive',
		@ActiveOnly = @ActiveAgreementsOnly
    SELECT
        ad.AgreementID,
        ad.OtcAccountId,
        ad.IAPledgedAccountId,
        ad.IASecuredAccountId,
        ad.VMPledgedAccountId,
        ad.VMSecuredAccountId,
		ad.RegIMPledgedAccountId, 
		ad.RegIMSecuredAccountId,
        ad.AgreementTypeCode,
        ad.CSAStatus,
        ad.IsActive
    INTO #AgreementDetailData
    FROM #GetAgreementDetail ad
    WHERE (@AgreementId IS NULL OR ad.AgreementId = @AgreementId)
        AND EXISTS(SELECT 1 FROM #CustodianAccount #ca WHERE #ca.CustodianAccountId_Final = ad.OtcAccountId)
    SET @SPCnt = @@ROWCOUNT;
    CREATE UNIQUE CLUSTERED INDEX PK_#AgreementDetailData ON #AgreementDetailData (AgreementId)
    SET @SPInfo =
        REPLICATE('    ', @@NESTLEVEL) +
        '#AgreementDetailData: inserted ' + CONVERT(VARCHAR(11), @SPCnt) + ' rows, time spent ' +
        CONVERT(VARCHAR(22), DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, @StartedAt, SYSDATETIME()), '00:00:00' ), 114)
    DROP TABLE IF EXISTS #GetAgreementDetail
	EXEC HTFSLog.dbo.SPTraceFinish @SPTraceId = @SubSPTraceId, @SPCnt = @SPCnt

    EXEC HTFSLog.dbo.SPTraceStart @SPDb = @SPDb, @SPName = @SPName, @SPTraceId = @SubSPTraceId OUTPUT, @SPSub = '#CustodianAccount_AgreementDetailData'

	SELECT CustodianAccountId_Final, AgreementId
	INTO #CustodianAccount_AgreementDetailData
	FROM 
	(
		SELECT ca.CustodianAccountId_Final, ad.AgreementId, rn = ROW_NUMBER() OVER(PARTITION BY ca.CustodianAccountId_Final ORDER BY ad.IsActive DESC, ad.AgreementId ASC)
		FROM #CustodianAccount ca
		JOIN #AgreementDetailData ad ON ca.CustodianAccountId_Final IN 
		    (ad.OtcAccountId, 
		     ad.IAPledgedAccountId, 
		     ad.IASecuredAccountId, 
		     ad.VMPledgedAccountId, 
		     ad.VMSecuredAccountId, 
		     ad.RegIMPledgedAccountId, 
		     ad.RegIMSecuredAccountId)
	)t
	WHERE t.rn = 1
    SET @SPCnt = @@ROWCOUNT;

	CREATE CLUSTERED INDEX PK_#CustodianAccount_AgreementDetailData ON #CustodianAccount_AgreementDetailData (CustodianAccountId_Final)
	EXEC HTFSLog.dbo.SPTraceFinish @SPTraceId = @SubSPTraceId, @SPCnt = @SPCnt

	IF @SPCnt > 0
	BEGIN
		EXEC HTFSLog.dbo.SPTraceStart @SPDb = @SPDb, @SPName = @SPName, @SPTraceId = @SubSPTraceId OUTPUT, @SPSub = '#CustodianAccount update AgreementId'

		SET @StartedAt = SYSDATETIME(); SET @SPCnt = 0
		UPDATE ca SET
		ca.AgreementId = ad.AgreementId
		FROM 
				#CustodianAccount ca
			JOIN #CustodianAccount_AgreementDetailData ad ON ca.CustodianAccountId_Final = ad.CustodianAccountId_Final

		SET @SPCnt = @@ROWCOUNT;
		SET @SPInfo =
			REPLICATE('    ', @@NESTLEVEL) +
			'#CustodianAccount.AgreementId: updated ' + CONVERT(VARCHAR(11), @SPCnt) + ' rows, time spent ' +
			CONVERT(VARCHAR(22), DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, @StartedAt, SYSDATETIME()), '00:00:00' ), 114)
		PRINT @SPInfo;
		EXEC HTFSLog.dbo.SPTraceInfo @SPTraceId = @SPTraceId, @SPInfo = @SPInfo
	END

	DROP TABLE IF EXISTS #CustodianAccount_AgreementDetailData

    SET @StartedAt = SYSDATETIME(); SET @SPCnt = 0
    DELETE FROM #CustodianAccount WHERE AgreementId IS NULL
    SET @SPCnt = @@ROWCOUNT;
    SET @SPInfo =
        REPLICATE('    ', @@NESTLEVEL) +
        '#CustodianAccount(AgreementId=NULL): deleted ' + CONVERT(VARCHAR(11), @SPCnt) + ' rows, time spent ' +
        CONVERT(VARCHAR(22), DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, @StartedAt, SYSDATETIME()), '00:00:00' ), 114)
    PRINT @SPInfo;
    EXEC HTFSLog.dbo.SPTraceInfo @SPTraceId = @SPTraceId, @SPInfo = @SPInfo



    SET @StartedAt = SYSDATETIME(); SET @SPCnt = 0
    EXEC HTFSLog.dbo.SPTraceStart @SPDb = @SPDb, @SPName = @SPName, @SPTraceId = @SubSPTraceId OUTPUT, @SPSub = '#CMSOverrideDetailData'

    SELECT
        -- PK
        od.PositionId,
		-----
		p.CompanyId,
        -- Overriden data
        AvgCostLocal            = od.NotionalAmountLocal, -- deprecated in old system though exists there and has values
        od.CallSubTypeId,
        od.ContractSize,
        od.FxRate,
        od.IMOverridePercent,
        InitialMarginBase       = od.InitialMargin,
        od.InitialMarginLocal,
        od.MaturityDate,
        od.MTMLocal,
        MTMBase                 = od.MTM,
        od.NaturalKey,
        NotionalBase            = ISNULL(od.NotionalAmountBase, 0),
        NotionalLocal           = ISNULL(od.NotionalAmountLocal, 0),
        PriceLocal              = ISNULL(od.Price, 0),
        od.PricingFactor,
--        od.SettlementDate,
        od.StrikePriceLocal,
--        od.TradeDate,
        TradingStrategyId       = ISNULL(od.TradingStrategyId, 0),
        -- service
        od.EffectiveDate,
        TransactionType         = (CASE WHEN ISNULL(od.NotionalAmountBase, 0) >= 0 THEN 'L' WHEN ISNULL(od.NotionalAmountBase, 0) < 0 THEN 'S' END),
        od.CMSOverrideId,
        od.CMSOverrideTypeId,
        od.CMSOverrideDateId,
        od.OverrideOperation,
        Exclude = ISNULL(od.Exclude, 0),
        #ca.AgreementId
    INTO #CMSOverrideDetailData
    FROM dbo.GetCMSOverRideDetails(@CompanyCode, @FromDate, NUll, NULL) od -- Override details
        INNER JOIN #CustodianAccount #ca ON #ca.CustodianAccountId = od.CustodianAccountId
        INNER JOIN dbo.Position p ON p.PositionId = od.PositionId
        LEFT JOIN #Position #p ON #p.PositionId = od.PositionId
    WHERE 1=1
        AND @VenderCode = 'HOUSE' -- not to extend breakdown for broker data with overrides
        AND od.EffectiveDate = @FromDate
        AND od.CMSOverrideTypeId = @CMSOverrideTypeId
--        AND (@RemoveExcluded = 0 OR ISNULL(od.Exclude, 0) <> 1)
        AND (@PositionIds IS NULL OR #p.PositionId IS NOT NULL)
    SET @SPCnt = @@ROWCOUNT;
    CREATE UNIQUE CLUSTERED INDEX PK_#CMSOverrideDetailData ON #CMSOverrideDetailData
        (PositionId)
    SET @SPInfo =
        REPLICATE('    ', @@NESTLEVEL) +
        '#CMSOverrideDetailData: inserted ' + CONVERT(VARCHAR(11), @SPCnt) + ' rows, time spent ' +
        CONVERT(VARCHAR(22), DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, @StartedAt, SYSDATETIME()), '00:00:00' ), 114)
    PRINT @SPInfo;
    EXEC HTFSLog.dbo.SPTraceFinish @SPTraceId = @SubSPTraceId, @SPCnt = @SPCnt

	CREATE TABLE #CMSOverrideDetailData_Prior
	(
		PositionId int primary key,
		InitialMarginBase decimal(28, 8),
		InitialMarginLocal decimal(28, 8),
		MTMLocal decimal(28, 8),
		MTMBase decimal(28, 8)
	)

	IF EXISTS(SELECT 1 FROM #Columns WHERE DisplayColumnName IN ('InternalPriorInitialMarginBase', 'InternalPriorInitialMarginLocal', 'InternalPriorMTMBase', 'InternalPriorMTMLocal', 'InternalPriorInitialMarginBaseDiff', 'InternalPriorInitialMarginLocalDiff', 'InternalPriorMTMBaseDiff', 'InternalPriorMTMLocalDiff'))
	BEGIN
		SET @StartedAt = SYSDATETIME(); SET @SPCnt = 0
		EXEC HTFSLog.dbo.SPTraceStart @SPDb = @SPDb, @SPName = @SPName, @SPTraceId = @SubSPTraceId OUTPUT, @SPSub = '#CMSOverrideDetailData_Prior'

		INSERT INTO #CMSOverrideDetailData_Prior(PositionId, InitialMarginBase, InitialMarginLocal, MTMLocal, MTMBase)
		SELECT
			-- PK
			od.PositionId,
			-----
			InitialMarginBase       = od.InitialMargin,
			InitialMarginLocal		= od.InitialMarginLocal,
			MTMLocal				= od.MTMLocal,
			MTMBase                 = od.MTM
		FROM dbo.GetCMSOverRideDetails(@CompanyCode, @FromDatePrior, NUll, NULL) od -- Override details
			INNER JOIN #CustodianAccount #ca ON #ca.CustodianAccountId = od.CustodianAccountId
			INNER JOIN dbo.Position p ON p.PositionId = od.PositionId
			LEFT JOIN #Position #p ON #p.PositionId = od.PositionId
		WHERE 1=1
			AND @VenderCode = 'HOUSE' -- not to extend breakdown for broker data with overrides
			AND od.EffectiveDate = @FromDatePrior
			AND od.CMSOverrideTypeId = @CMSOverrideTypeId
	--        AND (@RemoveExcluded = 0 OR ISNULL(od.Exclude, 0) <> 1)
			AND (@PositionIds IS NULL OR #p.PositionId IS NOT NULL)
		SET @SPCnt = @@ROWCOUNT;
		SET @SPInfo =
			REPLICATE('    ', @@NESTLEVEL) +
			'#CMSOverrideDetailData_Prior: inserted ' + CONVERT(VARCHAR(11), @SPCnt) + ' rows, time spent ' +
			CONVERT(VARCHAR(22), DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, @StartedAt, SYSDATETIME()), '00:00:00' ), 114)
		PRINT @SPInfo;
		EXEC HTFSLog.dbo.SPTraceFinish @SPTraceId = @SubSPTraceId, @SPCnt = @SPCnt
	END

    SET @StartedAt = SYSDATETIME(); SET @SPCnt = 0
    EXEC HTFSLog.dbo.SPTraceStart @SPDb = @SPDb, @SPName = @SPName, @SPTraceId = @SubSPTraceId OUTPUT, @SPSub = '#PositionId'

    SELECT od.PositionId, od.CompanyId
    INTO #PositionId
    FROM #CMSOverrideDetailData od
    WHERE (@RemoveExcluded = 0 OR od.Exclude = 0)
    UNION
    SELECT pca.PositionId, pca.CompanyId
    FROM dbo.PositionCollateralArchive pca
        INNER JOIN dbo.Position p
            ON p.PositionId = pca.PositionId
            AND p.CompanyId = pca.CompanyId
			AND p.PositionTypeId = @PositionTypeId -- here it usually refers to 'POS'
        LEFT JOIN #Position #p ON #p.PositionId = pca.PositionId
        INNER JOIN #CustodianAccount ca ON ca.CustodianAccountId = p.CustodianAccountId
        LEFT JOIN #CMSOverrideDetailData od_excluded
            ON od_excluded.PositionId = pca.PositionId
            AND od_excluded.Exclude = 1
    WHERE pca.Date = @FromDate
        AND (@PositionIds IS NULL OR #p.PositionId IS NOT NULL)
        AND pca.AgreementId IS NOT NULL
        AND (@RemoveExcluded = 0 OR od_excluded.PositionId IS NULL)
        AND pca.IsCollateralData = 1
    SET @SPCnt = @@ROWCOUNT;
    CREATE UNIQUE CLUSTERED INDEX PK_#PositionId ON #PositionId
        (CompanyId, PositionId)
    SET @SPInfo =
        REPLICATE('    ', @@NESTLEVEL) +
        '#PositionId: inserted ' + CONVERT(VARCHAR(11), @SPCnt) + ' rows, time spent ' +
        CONVERT(VARCHAR(22), DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, @StartedAt, SYSDATETIME()), '00:00:00' ), 114)
    PRINT @SPInfo;
    EXEC HTFSLog.dbo.SPTraceFinish @SPTraceId = @SubSPTraceId, @SPCnt = @SPCnt

	EXEC HTFSLog.dbo.SPTraceStart @SPDb = @SPDb, @SPName = @SPName, @SPTraceId = @SubSPTraceId OUTPUT, @SPSub = ' prepare sql'

	DECLARE @SelectColumns nvarchar(max) = NULL
	DECLARE @InsertColumns nvarchar(max)	= NULL

	SELECT @SelectColumns = ISNULL(@SelectColumns + ',', '') + DisplayColumnName + '=' + ColumnExpression + '
	'
	FROM #Columns
	ORDER BY Ord ASC

	SELECT @InsertColumns = ISNULL(@InsertColumns + ',', '') + DisplayColumnName
	FROM #Columns
	ORDER BY Ord ASC

	SET @sql = CONVERT(nvarchar(max), ' ')
	SET @sql = @sql + 
	CASE WHEN OBJECT_ID('tempdb..#GetOpenTradeEntities') IS NOT NULL 
		THEN 'INSERT INTO #GetOpenTradeEntities(' + @InsertColumns + ')' 
		ELSE '' 
	END + '
	SELECT ' + @SelectColumns + CONVERT(nvarchar(max), '
	FROM 
			#PositionId #p
        
		INNER LOOP JOIN dbo.Position p							ON p.PositionId = #p.PositionId
	    INNER JOIN #CustodianAccount #ca						ON #ca.CustodianAccountId = p.CustodianAccountId
        INNER JOIN dbo.CustodianAccount ca						ON ca.CustodianAccountId = p.CustodianAccountId
		INNER JOIN AccountType at								ON at.AccountTypeId = ca.AccountTypeId
        INNER JOIN dbo.Custodian cust							ON cust.CustodianId = ca.CustodianId        
        INNER JOIN dbo.Portfolio po								ON po.PortfolioId = ca.PortfolioId        
        INNER JOIN PositionTradingStrategy pts					ON pts.TradingStrategyId = p.TradingStrategyId
        INNER JOIN dbo.Currency ccy								ON ccy.CurrencyId = p.CurrencyId
        INNER JOIN dbo.Security s								ON s.SecurityId = p.SecurityId AND s.IsActive = 1
        INNER JOIN dbo.SecurityType st							ON st.SecurityTypeId = s.SecurityTypeId
		
		-- Not guaranteed to have a PositionDate entry if Override added (not edited)
        LEFT LOOP JOIN dbo.PositionDate pd						ON pd.PositionId = #p.PositionId AND pd.DateId = @FromDateId

        LEFT LOOP JOIN #CMSOverrideDetailData od				ON od.PositionId = #p.PositionId
		LEFT LOOP JOIN #CMSOverrideDetailData_Prior od_prior	ON od_prior.PositionId = #p.PositionId

        LEFT LOOP JOIN dbo.PositionGlobalArchive pga_h
            ON pga_h.Date = @FromDate
            AND pga_h.CompanyId = #p.CompanyId
			AND pga_h.PositionId = #p.PositionId
            AND pga_h.IsHouse = 1
            AND pga_h.Type = ''F''

        LEFT LOOP JOIN dbo.PositionGlobalArchive pga_b
            ON pga_b.Date = @FromDate
            AND pga_b.CompanyId = #p.CompanyId
			AND pga_b.PositionId = #p.PositionId
            AND pga_b.IsHouse = 0
            AND pga_b.Type = ''F''

		LEFT LOOP JOIN dbo.PositionGlobalArchive pga_h_prior
            ON pga_h_prior.Date = @FromDatePrior
            AND pga_h_prior.CompanyId = #p.CompanyId
			AND pga_h_prior.PositionId = #p.PositionId
            AND pga_h_prior.IsHouse = 1
            AND pga_h_prior.Type = ''F''

		LEFT LOOP JOIN dbo.PositionGlobalArchive pga_b_prior
            ON pga_b_prior.Date = @FromDatePrior
            AND pga_b_prior.CompanyId = #p.CompanyId
			AND pga_b_prior.PositionId = #p.PositionId
            AND pga_b_prior.IsHouse = 0
            AND pga_b_prior.Type = ''F''

        LEFT LOOP JOIN dbo.PositionCollateralArchive pca_h
            ON pca_h.Date = @FromDate
            AND pca_h.CompanyId = #p.CompanyId
			AND pca_h.PositionId = #p.PositionId
            AND pca_h.IsHouse = 1
            AND pca_h.Type = ''F''

        LEFT LOOP JOIN dbo.PositionCollateralArchive pca_b
            ON pca_b.Date = @FromDate
            AND pca_b.CompanyId = #p.CompanyId
			AND pca_b.PositionId = #p.PositionId
            AND pca_b.IsHouse = 0
            AND pca_b.Type = ''F''

		LEFT LOOP JOIN dbo.PositionCollateralArchive pca_h_prior
            ON pca_h_prior.Date = @FromDatePrior
            AND pca_h_prior.CompanyId = #p.CompanyId
			AND pca_h_prior.PositionId = #p.PositionId
            AND pca_h_prior.IsHouse = 1
            AND pca_h_prior.Type = ''F''

		 LEFT LOOP JOIN dbo.PositionCollateralArchive pca_b_prior
            ON pca_b_prior.Date = @FromDatePrior
            AND pca_b_prior.CompanyId = #p.CompanyId
			AND pca_b_prior.PositionId = #p.PositionId
            AND pca_b_prior.IsHouse = 0
            AND pca_b_prior.Type = ''F''
        
        LEFT LOOP JOIN dbo.PositionGlobalArchiveState pgas_h
            ON pgas_h.Date = @FromDate
            AND pgas_h.CompanyId = #p.CompanyId
			AND pgas_h.PositionId = #p.PositionId
            AND pgas_h.IsHouse = 1
            AND pgas_h.Type = ''F''

        LEFT LOOP JOIN dbo.PositionGlobalArchiveState pgas_b
            ON pgas_b.Date = @FromDate
            AND pgas_b.CompanyId = #p.CompanyId
			AND pgas_b.PositionId = #p.PositionId
            AND pgas_b.IsHouse = 0
            AND pgas_b.Type = ''F''

        LEFT LOOP JOIN dbo.PositionCollateralArchiveState pcas_h
            ON pcas_h.Date = @FromDate
            AND pcas_h.CompanyId = #p.CompanyId
			AND pcas_h.PositionId = #p.PositionId
            AND pcas_h.IsHouse = 1
            AND pcas_h.Type = ''F''

        LEFT LOOP JOIN dbo.PositionCollateralArchiveState pcas_b
            ON pcas_b.Date = @FromDate
            AND pcas_b.CompanyId = #p.CompanyId
			AND pcas_b.PositionId = #p.PositionId
            AND pcas_b.IsHouse = 0
            AND pcas_b.Type = ''F''
	   
        LEFT JOIN dbo.CustodianAccount ca_roll				ON ca_roll.CustodianAccountId = ca.RollupCustodianAccountId
        LEFT JOIN dbo.Company seccompany					ON seccompany.CompanyId = s.CompanyId

        LEFT JOIN dbo.SecurityIssuer si						ON si.SecurityIssuerId = s.SecurityIssuerId
        LEFT JOIN dbo.SecurityFixedIncomeSpecific sfis		ON sfis.SecurityId = p.SecurityId
        LEFT JOIN dbo.Country c								ON c.CountryId = s.CountryId
        -- underlying
        LEFT JOIN dbo.Security su							ON su.SecurityId = s.UnderlierSecurityId
        LEFT JOIN dbo.SecurityType stu						ON stu.SecurityTypeId = su.SecurityTypeId
        LEFT JOIN dbo.SecurityFixedIncomeSpecific sfisu		ON sfisu.SecurityId = s.UnderlierSecurityId
        LEFT JOIN dbo.Currency ccyu							ON ccyu.CurrencyId = su.CurrencyId
        LEFT JOIN dbo.Country cu							ON cu.CountryId = su.CountryId

        LEFT JOIN CallSubType cst							ON cst.CallSubTypeId = ISNULL(od.CallSubTypeId, pca_h.CallSubtypeId)
        LEFT JOIN #AgreementDetailData ad					ON ad.AgreementId = #ca.AgreementId
		LEFT JOIN dbo.PositionRefData prd					ON prd.PositionId = p.PositionId
		LEFT JOIN vw_PortfolioFocusManagerStyleTheme pa		ON pa.PortfolioId = po.PortfolioId

		CROSS APPLY(SELECT 
			OriginalFilled	= CASE WHEN COALESCE(NULLIF(s.ISIN, ''''),	NULLIF(s.Cusip, ''''),	NULLIF(s.Sedol, '''')) IS NOT NULL	THEN 1 ELSE 0 END,
			UnderlierFilled = CASE WHEN COALESCE(NULLIF(su.ISIN, ''''), NULLIF(su.Cusip, ''''), NULLIF(su.Sedol, '''')) IS NOT NULL THEN 1 ELSE 0 END
		)public_ids_mark(OriginalFilled, UnderlierFilled)

		CROSS APPLY(SELECT 
				FromUnderlier = CASE WHEN public_ids_mark.UnderlierFilled = 1 AND public_ids_mark.OriginalFilled = 0 THEN 1 ELSE 0 END
		)public_ids_use(FromUnderlier)

		CROSS APPLY(SELECT 
				ISIN		= CASE WHEN public_ids_use.FromUnderlier = 1 THEN su.ISIN			ELSE s.ISIN END,
				Cusip		= CASE WHEN public_ids_use.FromUnderlier = 1 THEN su.Cusip			ELSE s.Cusip END,
				Sedol		= CASE WHEN public_ids_use.FromUnderlier = 1 THEN su.Sedol			ELSE s.Sedol END,
				BbergCode	= CASE WHEN public_ids_use.FromUnderlier = 1 THEN su.BbergCode		ELSE s.BbergCode END,
				OMSTicker	= CASE WHEN public_ids_use.FromUnderlier = 1 THEN su.OMSTicker		ELSE s.OMSTicker END,
				ReutersCode = CASE WHEN public_ids_use.FromUnderlier = 1 THEN su.ReutersCode	ELSE s.ReutersCode END
		)public_ids(Isin, Cusip, Sedol, BbergCode, OMSTicker, ReutersCode)

		CROSS APPLY(SELECT
			MTMBase					= COALESCE(od.MTMBase, pga_h.MTMBase * pca_h.FxRatePortfolio2Agreement, pga_h.MTMBase, 0),
			MTMLocal				= COALESCE(od.MTMLocal, pga_h.MTMLocal, 0),
			IAPct					= COALESCE(pca_h.IAPct, IIF(pga_h.NotionalLocal = 0, 0, ABS(ABS(pca_h.InitialMarginLocal) / ABS(pga_h.NotionalLocal)))),
			NotionalAmountBase		= COALESCE(od.NotionalBase, pga_h.NotionalBase * pca_h.FxRatePortfolio2Agreement, pga_h.NotionalBase),
			NotionalAmountLocal		= ISNULL(od.NotionalLocal, pga_h.NotionalLocal),
			InitialMarginBase		= COALESCE(od.InitialMarginBase, pca_h.InitialMarginBase * pca_h.FxRatePortfolio2Agreement, pca_h.InitialMarginBase, 0),
			InitialMarginLocal		= COALESCE(od.InitialMarginLocal, pca_h.InitialMarginLocal, 0),
		
			PriorMTMBase			= COALESCE(od_prior.MTMBase, pga_h_prior.MTMBase * pca_h_prior.FxRatePortfolio2Agreement, pga_h_prior.MTMBase, 0),
			PriorMTMLocal			= COALESCE(od_prior.MTMLocal, pga_h_prior.MTMLocal, 0),
			PriorInitialMarginBase	= COALESCE(od_prior.InitialMarginBase, pca_h_prior.InitialMarginBase * pca_h_prior.FxRatePortfolio2Agreement, pca_h_prior.InitialMarginBase, 0),
			PriorInitialMarginLocal	= COALESCE(od_prior.InitialMarginLocal, pca_h_prior.InitialMarginLocal, 0)
		)Internal(MTMBase, MTMLocal, IAPct, NotionalAmountBase, NotionalAmountLocal, InitialMarginBase, InitialMarginLocal, PriorMTMBase, PriorMTMLocal, PriorInitialMarginBase, PriorInitialMarginLocal) 

		CROSS APPLY(SELECT
			MTMBase					= COALESCE(pga_b.MTMBase * pca_h.FxRatePortfolio2Agreement, pga_b.MTMBase, 0),
			MTMLocal				= ISNULL(pga_b.MTMLocal, 0),
			IAPct					= COALESCE(pca_b.IAPct, IIF(pga_b.NotionalLocal = 0, 0, ABS(ABS(pca_b.InitialMarginLocal) / ABS(pga_b.NotionalLocal)))),
			NotionalAmountBase		= COALESCE(pga_b.NotionalBase * pca_h.FxRatePortfolio2Agreement, pga_b.NotionalBase),
			NotionalAmountLocal		= pga_b.NotionalLocal,
			InitialMarginBase		= COALESCE(pca_b.InitialMarginBase * pca_h.FxRatePortfolio2Agreement, pca_b.InitialMarginBase, 0),
			InitialMarginLocal		= ISNULL(pca_b.InitialMarginLocal, 0),
		
			PriorMTMBase			= COALESCE(pga_b_prior.MTMBase * pca_h_prior.FxRatePortfolio2Agreement, pga_b_prior.MTMBase, 0),
			PriorMTMLocal			= ISNULL(pga_b_prior.MTMLocal, 0),
			PriorInitialMarginBase	= COALESCE(pca_b_prior.InitialMarginBase * pca_h_prior.FxRatePortfolio2Agreement, pca_b_prior.InitialMarginBase, 0),
			PriorInitialMarginLocal	= ISNULL(pca_b_prior.InitialMarginLocal, 0)
		)Counterparty(MTMBase, MTMLocal, IAPct, NotionalAmountBase, NotionalAmountLocal, InitialMarginBase, InitialMarginLocal, PriorMTMBase, PriorMTMLocal, PriorInitialMarginBase, PriorInitialMarginLocal)
	')

--		exec HTFSUtilities.ut.PrintAll @sql
	EXEC HTFSLog.dbo.SPTraceFinish @SPTraceId = @SubSPTraceId, @SPInfo = @sql

	SET @StartedAt = SYSDATETIME(); SET @SPCnt = 0
	
	EXEC HTFSLog.dbo.SPTraceStart @SPDb = @SPDb, @SPName = @SPName, @SPTraceId = @SubSPTraceId OUTPUT, @SPSub = ' exec sql'

	EXEC sp_executesql @sql, N'@FromDate datetime, @FromDateId int, @PositionTypeCode varchar(20), @FromDatePrior date', @FromDate=@FromDate, @FromDateId=@FromDateId, @PositionTypeCode=@PositionTypeCode, @FromDatePrior=@FromDatePrior

	SET @SPCnt = @@rowcount
    SET @SPInfo =
        REPLICATE('    ', @@NESTLEVEL) +
        '#GetOpenTradeEntities: inserted ' + CONVERT(VARCHAR(11), @SPCnt) + ' rows, time spent ' +
        CONVERT(VARCHAR(22), DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, @StartedAt, SYSDATETIME()), '00:00:00' ), 114)
    PRINT @SPInfo
    EXEC HTFSLog.dbo.SPTraceFinish @SPTraceId = @SubSPTraceId, @SPCnt = @SPCnt

	END TRY
	BEGIN CATCH
		EXEC HTFSLog.dbo.RaiseLastError @SPTraceId = @SPTraceId, @SPName = @SPName, @SPInfo = @SPInfo
		RETURN
	END CATCH
	
    EXEC HTFSLog.dbo.SPTraceFinish @SPTraceId = @SPTraceId, @SPCnt = @SPCnt

END

GO
