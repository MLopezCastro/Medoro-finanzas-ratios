/* ============================================================
   MEDORO FINANZAS – SCRIPT MAESTRO (REPLICA / PROD)
   DB: Medoro_Finanzas_Ratios
   Fuente: CSV en carpeta staging_csv
   Ejecutar TODO el script (incluye 1 solo GO).
   ============================================================ */

SET NOCOUNT ON;

---------------------------------------------------------------
-- A) CREAR DB (primer batch)
---------------------------------------------------------------
IF DB_ID('Medoro_Finanzas_Ratios') IS NULL
BEGIN
    CREATE DATABASE Medoro_Finanzas_Ratios;
END;
GO

---------------------------------------------------------------
-- B) TODO LO DEMÁS VA EN UN ÚNICO BATCH (NO MÁS GO)
---------------------------------------------------------------
USE Medoro_Finanzas_Ratios;
SET NOCOUNT ON;

---------------------------------------------------------------
-- B.1) SCHEMAS
---------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');

---------------------------------------------------------------
-- B.2) RUTA CSV (ÚNICO PUNTO A EDITAR EN OTRAS PCs)------------------------------------------------VER ESTO---------------------CORRER ANTES RUN Y USAR CARPETA CSVS
---------------------------------------------------------------
DECLARE @RutaCSV NVARCHAR(500) = N'C:\Users\mlope\OneDrive\Escritorio\medoro_finanzas_pipeline\staging_csv\';
DECLARE @sql NVARCHAR(MAX);

---------------------------------------------------------------
-- Helper: asegura barra final
---------------------------------------------------------------
IF RIGHT(@RutaCSV, 1) <> N'\'
    SET @RutaCSV = @RutaCSV + N'\';

---------------------------------------------------------------
-- 1) CHEQUES RECIBIDOS  (CHEQUES.csv)
---------------------------------------------------------------
PRINT '1) CHEQUES RECIBIDOS - BRONZE';
IF OBJECT_ID('bronze.Cheques_Recibidos_Raw','U') IS NOT NULL DROP TABLE bronze.Cheques_Recibidos_Raw;

CREATE TABLE bronze.Cheques_Recibidos_Raw
(
    NOMBRE     NVARCHAR(100) NULL,
    DEUDOR     NVARCHAR(200) NULL,
    N_CHEQUE   NVARCHAR(50)  NULL,
    ECHEQ      NVARCHAR(10)  NULL,
    VTO        NVARCHAR(50)  NULL,
    IMPORTE    NVARCHAR(50)  NULL
);

SET @sql = N'
BULK INSERT bronze.Cheques_Recibidos_Raw
FROM ''' + REPLACE(@RutaCSV,'''','''''') + N'CHEQUES.csv''
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '','',
    ROWTERMINATOR   = ''0x0a'',
    CODEPAGE        = ''65001'',
    TABLOCK
);';
EXEC sys.sp_executesql @sql;

PRINT '1) CHEQUES RECIBIDOS - SILVER';
IF OBJECT_ID('silver.Cheques_Recibidos','U') IS NOT NULL DROP TABLE silver.Cheques_Recibidos;

CREATE TABLE silver.Cheques_Recibidos
(
    IdChequesRecibidos INT IDENTITY(1,1) PRIMARY KEY,
    EMPRESA      NVARCHAR(100)  NOT NULL,
    CLIENTE      NVARCHAR(200)  NOT NULL,
    NUM_CHEQUE   NVARCHAR(50)   NOT NULL,
    ECHEQ        BIT            NOT NULL,
    FECHA_VTO    DATE           NULL,
    IMPORTE      DECIMAL(18,2)  NOT NULL,
    FUENTE       NVARCHAR(50)   NOT NULL
);

INSERT INTO silver.Cheques_Recibidos
(EMPRESA, CLIENTE, NUM_CHEQUE, ECHEQ, FECHA_VTO, IMPORTE, FUENTE)
SELECT
    LTRIM(RTRIM(NOMBRE)),
    LTRIM(RTRIM(DEUDOR)),
    LTRIM(RTRIM(N_CHEQUE)),
    CASE UPPER(LTRIM(RTRIM(ECHEQ)))
        WHEN 'TRUE'  THEN CONVERT(bit,1)
        WHEN 'FALSE' THEN CONVERT(bit,0)
        WHEN 'SI'    THEN CONVERT(bit,1)
        WHEN 'NO'    THEN CONVERT(bit,0)
        WHEN '1'     THEN CONVERT(bit,1)
        WHEN '0'     THEN CONVERT(bit,0)
        ELSE CONVERT(bit,0)
    END,
    COALESCE(
        TRY_CONVERT(date, LTRIM(RTRIM(VTO)), 103),
        TRY_CONVERT(date, LTRIM(RTRIM(VTO)), 101),
        TRY_CONVERT(date, LTRIM(RTRIM(VTO)), 23)
    ),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(LTRIM(RTRIM(IMPORTE)),'.',''), ',', '.')),
    'CHEQUES_RECIBIDOS'
FROM bronze.Cheques_Recibidos_Raw
WHERE TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(LTRIM(RTRIM(IMPORTE)),'.',''), ',', '.')) IS NOT NULL;

---------------------------------------------------------------
-- 2) CHEQUES EMITIDOS (CHEQUES_EMITIDOS_2.csv)
---------------------------------------------------------------
PRINT '2) CHEQUES EMITIDOS - BRONZE';
IF OBJECT_ID('bronze.Cheques_Emitidos_Raw','U') IS NOT NULL DROP TABLE bronze.Cheques_Emitidos_Raw;

CREATE TABLE bronze.Cheques_Emitidos_Raw
(
    EMPRESA        NVARCHAR(200) NULL,
    BANCO          NVARCHAR(50)  NULL,
    CHEQUE         NVARCHAR(50)  NULL,
    BENEFICIAR     NVARCHAR(300) NULL,
    EMISION        NVARCHAR(50)  NULL,
    VTO            NVARCHAR(50)  NULL,
    IMPORTE        NVARCHAR(50)  NULL
);

SET @sql = N'
BULK INSERT bronze.Cheques_Emitidos_Raw
FROM ''' + REPLACE(@RutaCSV,'''','''''') + N'CHEQUES_EMITIDOS_2.csv''
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '','',
    ROWTERMINATOR   = ''0x0a'',
    CODEPAGE        = ''65001'',
    TABLOCK
);';
EXEC sys.sp_executesql @sql;

PRINT '2) CHEQUES EMITIDOS - SILVER';
IF OBJECT_ID('silver.Cheques_Emitidos','U') IS NOT NULL DROP TABLE silver.Cheques_Emitidos;

CREATE TABLE silver.Cheques_Emitidos
(
    IdChequeEmitido INT IDENTITY(1,1) PRIMARY KEY,
    EMPRESA         NVARCHAR(200) NOT NULL,
    BANCO_CODIGO    NVARCHAR(50)  NOT NULL,
    NUM_CHEQUE      NVARCHAR(50)  NOT NULL,
    PROVEEDOR       NVARCHAR(300) NOT NULL,
    FECHA_EMISION   DATE          NULL,
    FECHA_VTO       DATE          NULL,
    IMPORTE         DECIMAL(18,2) NOT NULL,
    ESTADO_CHEQUE   NVARCHAR(20)  NOT NULL,
    FUENTE          NVARCHAR(50)  NOT NULL
);

INSERT INTO silver.Cheques_Emitidos
(EMPRESA, BANCO_CODIGO, NUM_CHEQUE, PROVEEDOR, FECHA_EMISION, FECHA_VTO, IMPORTE, ESTADO_CHEQUE, FUENTE)
SELECT
    LTRIM(RTRIM(EMPRESA)),
    LTRIM(RTRIM(BANCO)),
    LTRIM(RTRIM(CHEQUE)),
    LTRIM(RTRIM(BENEFICIAR)),
    COALESCE(TRY_CONVERT(date, EMISION, 103), TRY_CONVERT(date, EMISION, 101), TRY_CONVERT(date, EMISION, 23)),
    COALESCE(TRY_CONVERT(date, VTO,     103), TRY_CONVERT(date, VTO,     101), TRY_CONVERT(date, VTO,     23)),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(LTRIM(RTRIM(IMPORTE)),'.',''), ',', '.')),
    CASE
        WHEN COALESCE(TRY_CONVERT(date, VTO, 103), TRY_CONVERT(date, VTO, 101), TRY_CONVERT(date, VTO, 23)) < CAST(GETDATE() AS date)
            THEN 'VENCIDO'
        ELSE 'PENDIENTE'
    END,
    'CHEQUES_EMITIDOS'
FROM bronze.Cheques_Emitidos_Raw
WHERE TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(LTRIM(RTRIM(IMPORTE)),'.',''), ',', '.')) IS NOT NULL;

---------------------------------------------------------------
-- 3) CTACTE PROVEEDOR (CTACTE_PROVEEDOR.csv)
---------------------------------------------------------------
PRINT '3) CTACTE PROVEEDOR - BRONZE';
IF OBJECT_ID('bronze.Ctacte_Proveedor_Raw','U') IS NOT NULL DROP TABLE bronze.Ctacte_Proveedor_Raw;

CREATE TABLE bronze.Ctacte_Proveedor_Raw
(
    NOM_EMP      NVARCHAR(100) NULL,
    PROVEEDOR    NVARCHAR(200) NULL,
    FACTURA      NVARCHAR(50)  NULL,
    EMISION      NVARCHAR(50)  NULL,
    VTO          NVARCHAR(50)  NULL,
    MON          NVARCHAR(10)  NULL,
    TC           NVARCHAR(50)  NULL,
    IMPORTE      NVARCHAR(50)  NULL,
    DEUDA_IMPORT NVARCHAR(20)  NULL
);

SET @sql = N'
BULK INSERT bronze.Ctacte_Proveedor_Raw
FROM ''' + REPLACE(@RutaCSV,'''','''''') + N'CTACTE_PROVEEDOR.csv''
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '','',
    ROWTERMINATOR   = ''0x0a'',
    CODEPAGE        = ''65001'',
    TABLOCK
);';
EXEC sys.sp_executesql @sql;

PRINT '3) CTACTE PROVEEDOR - SILVER';
IF OBJECT_ID('silver.Ctacte_Proveedor','U') IS NOT NULL DROP TABLE silver.Ctacte_Proveedor;

CREATE TABLE silver.Ctacte_Proveedor
(
    IdCtacteProveedor INT IDENTITY(1,1) PRIMARY KEY,
    EMPRESA         NVARCHAR(100)  NOT NULL,
    PROVEEDOR       NVARCHAR(200)  NOT NULL,
    ES_BANCO        BIT            NOT NULL,
    NUM_FACTURA     NVARCHAR(50)   NOT NULL,
    FECHA_EMISION   DATE           NULL,
    FECHA_VTO       DATE           NULL,
    MONEDA          NVARCHAR(10)   NOT NULL,
    TIPO_CAMBIO     DECIMAL(18,4)  NULL,
    IMPORTE         DECIMAL(18,2)  NOT NULL,
    DEUDA_IMPORT    BIT            NOT NULL,
    IMPORTE_TOTAL   DECIMAL(18,2)  NOT NULL,
    FUENTE          NVARCHAR(50)   NOT NULL
);

INSERT INTO silver.Ctacte_Proveedor
(EMPRESA, PROVEEDOR, ES_BANCO, NUM_FACTURA, FECHA_EMISION, FECHA_VTO, MONEDA, TIPO_CAMBIO, IMPORTE, DEUDA_IMPORT, IMPORTE_TOTAL, FUENTE)
SELECT
    LTRIM(RTRIM(NOM_EMP)),
    LTRIM(RTRIM(PROVEEDOR)),
    CASE WHEN UPPER(LTRIM(RTRIM(PROVEEDOR))) LIKE 'BANCO %' THEN CONVERT(bit,1) ELSE CONVERT(bit,0) END,
    LTRIM(RTRIM(FACTURA)),
    COALESCE(TRY_CONVERT(date, EMISION, 103), TRY_CONVERT(date, EMISION, 101), TRY_CONVERT(date, EMISION, 23)),
    COALESCE(TRY_CONVERT(date, VTO,     103), TRY_CONVERT(date, VTO,     101), TRY_CONVERT(date, VTO,     23)),
    LTRIM(RTRIM(MON)),
    TRY_CONVERT(DECIMAL(18,4), REPLACE(REPLACE(LTRIM(RTRIM(TC)),'.',''), ',', '.')),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(LTRIM(RTRIM(IMPORTE)),'.',''), ',', '.')),
    CASE WHEN UPPER(LTRIM(RTRIM(DEUDA_IMPORT))) IN ('TRUE','SI','YES','1') THEN CONVERT(bit,1) ELSE CONVERT(bit,0) END,
    CASE
        WHEN UPPER(LTRIM(RTRIM(MON))) IN ('DOL','USD')
            THEN TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(LTRIM(RTRIM(IMPORTE)),'.',''), ',', '.'))
                 * ISNULL(TRY_CONVERT(DECIMAL(18,4), REPLACE(REPLACE(LTRIM(RTRIM(TC)),'.',''), ',', '.')), 1)
        ELSE TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(LTRIM(RTRIM(IMPORTE)),'.',''), ',', '.'))
    END,
    'CTACTE_PROVEEDOR'
FROM bronze.Ctacte_Proveedor_Raw
WHERE TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(LTRIM(RTRIM(IMPORTE)),'.',''), ',', '.')) IS NOT NULL;

---------------------------------------------------------------
-- 4) EXTRA RATIOS (EXTRA_RATIOS.csv)
---------------------------------------------------------------
PRINT '4) EXTRA RATIOS - BRONZE';
IF OBJECT_ID('bronze.EXTRA_RATIOS_Raw','U') IS NOT NULL DROP TABLE bronze.EXTRA_RATIOS_Raw;

CREATE TABLE bronze.EXTRA_RATIOS_Raw
(
    EMPRESA            VARCHAR(200),
    EFECTIVO           VARCHAR(50),
    CUENTAS_POR_COBRAR VARCHAR(50),
    INVENTARIOS        VARCHAR(50)
);

SET @sql = N'
BULK INSERT bronze.EXTRA_RATIOS_Raw
FROM ''' + REPLACE(@RutaCSV,'''','''''') + N'EXTRA_RATIOS.csv''
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '','',
    ROWTERMINATOR   = ''0x0a'',
    CODEPAGE        = ''65001'',
    TABLOCK
);';
EXEC sys.sp_executesql @sql;

PRINT '4) EXTRA RATIOS - SILVER';
IF OBJECT_ID('silver.EXTRA_RATIOS','U') IS NOT NULL DROP TABLE silver.EXTRA_RATIOS;

CREATE TABLE silver.EXTRA_RATIOS
(
    ID                 INT IDENTITY(1,1) PRIMARY KEY,
    EMPRESA            VARCHAR(200) NOT NULL,
    EFECTIVO           DECIMAL(18,2),
    CUENTAS_POR_COBRAR DECIMAL(18,2),
    INVENTARIOS        DECIMAL(18,2),
    FUENTE             VARCHAR(50)
);

INSERT INTO silver.EXTRA_RATIOS (EMPRESA, EFECTIVO, CUENTAS_POR_COBRAR, INVENTARIOS, FUENTE)
SELECT
    LTRIM(RTRIM(EMPRESA)),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(EFECTIVO)),''),'.',''), ',', '.')),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(CUENTAS_POR_COBRAR)),''),'.',''), ',', '.')),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(INVENTARIOS)),''),'.',''), ',', '.')),
    'EXTRA_RATIOS'
FROM bronze.EXTRA_RATIOS_Raw;

---------------------------------------------------------------
-- 5) IMPORTACIONES ADUANA (IMPORTACIONES_ADUANA.csv)
---------------------------------------------------------------
PRINT '5) IMPORTACIONES ADUANA - BRONZE';
IF OBJECT_ID('bronze.Importaciones_Aduana_Raw','U') IS NOT NULL DROP TABLE bronze.Importaciones_Aduana_Raw;

CREATE TABLE bronze.Importaciones_Aduana_Raw
(
    EMPRESA          VARCHAR(200),
    PRODUCTO         VARCHAR(200),
    ADUANA_PESOS     VARCHAR(50),
    GASTOS_NAC_PESOS VARCHAR(50),
    FECHA_ARRIBO     VARCHAR(20)
);

SET @sql = N'
BULK INSERT bronze.Importaciones_Aduana_Raw
FROM ''' + REPLACE(@RutaCSV,'''','''''') + N'IMPORTACIONES_ADUANA.csv''
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '','',
    ROWTERMINATOR   = ''0x0a'',
    CODEPAGE        = ''65001'',
    TABLOCK
);';
EXEC sys.sp_executesql @sql;

PRINT '5) IMPORTACIONES ADUANA - SILVER';
IF OBJECT_ID('silver.Importaciones_Aduana','U') IS NOT NULL DROP TABLE silver.Importaciones_Aduana;

CREATE TABLE silver.Importaciones_Aduana
(
    ID               INT IDENTITY(1,1) PRIMARY KEY,
    EMPRESA          VARCHAR(200),
    PRODUCTO         VARCHAR(200),
    ADUANA_PESOS     DECIMAL(18,2),
    GASTOS_NAC_PESOS DECIMAL(18,2),
    FECHA_ARRIBO     DATE,
    FUENTE           VARCHAR(50)
);

INSERT INTO silver.Importaciones_Aduana
(EMPRESA, PRODUCTO, ADUANA_PESOS, GASTOS_NAC_PESOS, FECHA_ARRIBO, FUENTE)
SELECT
    NULLIF(LTRIM(RTRIM(EMPRESA)), ''),
    NULLIF(LTRIM(RTRIM(PRODUCTO)), ''),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(ADUANA_PESOS)),''),'.',''), ',', '.')),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(GASTOS_NAC_PESOS)),''),'.',''), ',', '.')),
    COALESCE(
        TRY_CONVERT(date, LTRIM(RTRIM(FECHA_ARRIBO)), 103),
        TRY_CONVERT(date, LTRIM(RTRIM(FECHA_ARRIBO)), 101),
        TRY_CONVERT(date, LTRIM(RTRIM(FECHA_ARRIBO)), 23)
    ),
    'IMPORTACIONES_ADUANA'
FROM bronze.Importaciones_Aduana_Raw;

---------------------------------------------------------------
-- 6) IMPUESTOS (IMPUESTOS.csv)
---------------------------------------------------------------
PRINT '6) IMPUESTOS - BRONZE';
IF OBJECT_ID('bronze.Impuestos_Raw','U') IS NOT NULL DROP TABLE bronze.Impuestos_Raw;

CREATE TABLE bronze.Impuestos_Raw
(
    FECHA_CARGA VARCHAR(20),
    EMPRESA     VARCHAR(50),
    IMPUESTO    VARCHAR(200),
    VENCIMIENTO VARCHAR(20),
    IMPORTE     VARCHAR(50),
    MES         VARCHAR(10),
    ANIO        VARCHAR(10)
);

SET @sql = N'
BULK INSERT bronze.Impuestos_Raw
FROM ''' + REPLACE(@RutaCSV,'''','''''') + N'IMPUESTOS.csv''
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '','',
    ROWTERMINATOR   = ''0x0a'',
    CODEPAGE        = ''65001'',
    TABLOCK
);';
EXEC sys.sp_executesql @sql;

PRINT '6) IMPUESTOS - SILVER';
IF OBJECT_ID('silver.Impuestos','U') IS NOT NULL DROP TABLE silver.Impuestos;

CREATE TABLE silver.Impuestos
(
    ID          INT IDENTITY(1,1) PRIMARY KEY,
    FECHA_CARGA DATE,
    EMPRESA     VARCHAR(50),
    IMPUESTO    VARCHAR(150),
    FECHA_VTO   DATE,
    IMPORTE     DECIMAL(18,2),
    MES         TINYINT,
    ANIO        SMALLINT,
    FUENTE      VARCHAR(20)
);

INSERT INTO silver.Impuestos
(FECHA_CARGA, EMPRESA, IMPUESTO, FECHA_VTO, IMPORTE, MES, ANIO, FUENTE)
SELECT
    COALESCE(TRY_CONVERT(date, FECHA_CARGA, 103), TRY_CONVERT(date, FECHA_CARGA, 101), TRY_CONVERT(date, FECHA_CARGA, 23)),
    UPPER(LTRIM(RTRIM(EMPRESA))),
    LTRIM(RTRIM(IMPUESTO)),
    COALESCE(TRY_CONVERT(date, VENCIMIENTO, 103), TRY_CONVERT(date, VENCIMIENTO, 101), TRY_CONVERT(date, VENCIMIENTO, 23)),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(LTRIM(RTRIM(IMPORTE)),'.',''), ',', '.')),
    TRY_CONVERT(TINYINT, MES),
    TRY_CONVERT(SMALLINT, ANIO),
    'IMPUESTOS'
FROM bronze.Impuestos_Raw;

---------------------------------------------------------------
-- 7) INTERESES (INTERESES.csv)
---------------------------------------------------------------
PRINT '7) INTERESES - BRONZE';
IF OBJECT_ID('bronze.Intereses_Raw','U') IS NOT NULL DROP TABLE bronze.Intereses_Raw;

CREATE TABLE bronze.Intereses_Raw
(
    EMPRESA   NVARCHAR(50)  NULL,
    BANCO     NVARCHAR(100) NULL,
    PRESTAMO  NVARCHAR(50)  NULL,
    CUOTAS    NVARCHAR(10)  NULL,
    INTERES   NVARCHAR(50)  NULL,
    VTO_CUOTA NVARCHAR(50)  NULL
);

SET @sql = N'
BULK INSERT bronze.Intereses_Raw
FROM ''' + REPLACE(@RutaCSV,'''','''''') + N'INTERESES.csv''
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '','',
    ROWTERMINATOR   = ''0x0a'',
    CODEPAGE        = ''65001'',
    TABLOCK
);';
EXEC sys.sp_executesql @sql;

PRINT '7) INTERESES - SILVER';
IF OBJECT_ID('silver.Intereses','U') IS NOT NULL DROP TABLE silver.Intereses;

CREATE TABLE silver.Intereses
(
    ID               INT IDENTITY(1,1) PRIMARY KEY,
    EMPRESA          NVARCHAR(50)   NOT NULL,
    BANCO            NVARCHAR(100)  NOT NULL,
    NRO_PRESTAMO     NVARCHAR(100)  NOT NULL,
    CUOTAS_TOTALES   INT            NOT NULL,
    INTERES_CUOTA    DECIMAL(18,2)  NOT NULL,
    FECHA_VTO_CUOTA  DATE           NOT NULL,
    FUENTE           NVARCHAR(50)   NOT NULL
);

INSERT INTO silver.Intereses
(EMPRESA, BANCO, NRO_PRESTAMO, CUOTAS_TOTALES, INTERES_CUOTA, FECHA_VTO_CUOTA, FUENTE)
SELECT
    LTRIM(RTRIM(EMPRESA)),
    LTRIM(RTRIM(BANCO)),
    LTRIM(RTRIM(PRESTAMO)),
    TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(CUOTAS)), '')),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(INTERES)),''),'.',''), ',', '.')),
    COALESCE(
        TRY_CONVERT(date, LTRIM(RTRIM(VTO_CUOTA)), 103),
        TRY_CONVERT(date, LTRIM(RTRIM(VTO_CUOTA)), 101),
        TRY_CONVERT(date, LTRIM(RTRIM(VTO_CUOTA)), 23)
    ),
    'INTERESES'
FROM bronze.Intereses_Raw
WHERE
    TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(CUOTAS)), '')) IS NOT NULL
    AND TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(INTERES)),''),'.',''), ',', '.')) IS NOT NULL
    AND COALESCE(
            TRY_CONVERT(date, LTRIM(RTRIM(VTO_CUOTA)), 103),
            TRY_CONVERT(date, LTRIM(RTRIM(VTO_CUOTA)), 101),
            TRY_CONVERT(date, LTRIM(RTRIM(VTO_CUOTA)), 23)
        ) IS NOT NULL;

---------------------------------------------------------------
-- 8) PROYECCION COBRANZAS (PROYECCION_COBRANZAS.csv)
---------------------------------------------------------------
PRINT '8) PROYECCION COBRANZAS - BRONZE';
IF OBJECT_ID('bronze.Proyeccion_Cobranzas_Raw','U') IS NOT NULL DROP TABLE bronze.Proyeccion_Cobranzas_Raw;

CREATE TABLE bronze.Proyeccion_Cobranzas_Raw
(
    EMP_NOM    NVARCHAR(200) NULL,
    NOM_CLI    NVARCHAR(300) NULL,
    NUMERO     NVARCHAR(50)  NULL,
    VENDEDOR   NVARCHAR(50)  NULL,
    FECHA      NVARCHAR(50)  NULL,
    DIAS_PLAZO NVARCHAR(50)  NULL,
    _PP        NVARCHAR(50)  NULL,
    MONEDA     NVARCHAR(50)  NULL,
    COTIZACION NVARCHAR(50)  NULL,
    TOTAL      NVARCHAR(50)  NULL,
    A_COBRAR   NVARCHAR(50)  NULL
);

SET @sql = N'
BULK INSERT bronze.Proyeccion_Cobranzas_Raw
FROM ''' + REPLACE(@RutaCSV,'''','''''') + N'PROYECCION_COBRANZAS.csv''
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '','',
    ROWTERMINATOR   = ''0x0a'',
    CODEPAGE        = ''65001'',
    TABLOCK
);';
EXEC sys.sp_executesql @sql;

PRINT '8) PROYECCION COBRANZAS - SILVER';
IF OBJECT_ID('silver.Proyeccion_Cobranzas','U') IS NOT NULL DROP TABLE silver.Proyeccion_Cobranzas;

CREATE TABLE silver.Proyeccion_Cobranzas
(
    IdProyeccionCobranzas INT IDENTITY(1,1) PRIMARY KEY,
    EMPRESA          NVARCHAR(200) NOT NULL,
    CLIENTE          NVARCHAR(300) NOT NULL,
    NUM_FACTURA      NVARCHAR(50)  NOT NULL,
    COD_VENDEDOR     NVARCHAR(20)  NULL,
    FECHA_EMISION    DATE          NULL,
    DIAS_PLAZO       INT           NULL,
    DIAS_PRIMER_PAGO INT           NULL,
    MONEDA           NVARCHAR(20)  NOT NULL,
    TIPO_CAMBIO      DECIMAL(18,6) NULL,
    IMPORTE_TOTAL    DECIMAL(18,2) NOT NULL,
    IMPORTE_A_COBRAR DECIMAL(18,2) NOT NULL,
    FUENTE           NVARCHAR(50)  NOT NULL
);

INSERT INTO silver.Proyeccion_Cobranzas
(EMPRESA, CLIENTE, NUM_FACTURA, COD_VENDEDOR, FECHA_EMISION, DIAS_PLAZO, DIAS_PRIMER_PAGO, MONEDA, TIPO_CAMBIO, IMPORTE_TOTAL, IMPORTE_A_COBRAR, FUENTE)
SELECT
    LTRIM(RTRIM(EMP_NOM)),
    LTRIM(RTRIM(NOM_CLI)),
    LTRIM(RTRIM(NUMERO)),
    NULLIF(LTRIM(RTRIM(VENDEDOR)), ''),
    COALESCE(TRY_CONVERT(date, FECHA, 103), TRY_CONVERT(date, FECHA, 101), TRY_CONVERT(date, FECHA, 23)),
    TRY_CONVERT(INT, NULLIF(DIAS_PLAZO,'')),
    TRY_CONVERT(INT, NULLIF(_PP,'')),
    LTRIM(RTRIM(MONEDA)),
    TRY_CONVERT(DECIMAL(18,6), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(COTIZACION)),''),'.',''), ',', '.')),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(TOTAL)),''),'.',''), ',', '.')),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(A_COBRAR)),''),'.',''), ',', '.')),
    'PROYECCION_COBRANZAS'
FROM bronze.Proyeccion_Cobranzas_Raw
WHERE TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(TOTAL)),''),'.',''), ',', '.')) IS NOT NULL;

---------------------------------------------------------------
-- 9) PROYECCION SUELDOS (PROYECCION_SUELDOS.csv)
---------------------------------------------------------------
PRINT '9) PROYECCION SUELDOS - BRONZE';
IF OBJECT_ID('bronze.Proyeccion_Sueldos_Raw','U') IS NOT NULL DROP TABLE bronze.Proyeccion_Sueldos_Raw;

CREATE TABLE bronze.Proyeccion_Sueldos_Raw
(
    EMPRESA   VARCHAR(50)  NULL,
    RUBRO     VARCHAR(50)  NULL,
    SUB_RUBRO VARCHAR(50)  NULL,
    DETALLE   VARCHAR(200) NULL,
    FECHA     VARCHAR(20)  NULL,
    IMPORTE   VARCHAR(50)  NULL
);

SET @sql = N'
BULK INSERT bronze.Proyeccion_Sueldos_Raw
FROM ''' + REPLACE(@RutaCSV,'''','''''') + N'PROYECCION_SUELDOS.csv''
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '','',
    ROWTERMINATOR   = ''0x0a'',
    CODEPAGE        = ''65001'',
    TABLOCK
);';
EXEC sys.sp_executesql @sql;

PRINT '9) PROYECCION SUELDOS - SILVER';
IF OBJECT_ID('silver.Proyeccion_Sueldos','U') IS NOT NULL DROP TABLE silver.Proyeccion_Sueldos;

CREATE TABLE silver.Proyeccion_Sueldos
(
    ID        INT IDENTITY(1,1) PRIMARY KEY,
    EMPRESA   VARCHAR(50)   NOT NULL,
    RUBRO     VARCHAR(50)   NOT NULL,
    SUB_RUBRO VARCHAR(50)   NULL,
    DETALLE   VARCHAR(200)  NULL,
    FECHA     DATE          NULL,
    IMPORTE   DECIMAL(18,2) NULL,
    FUENTE    VARCHAR(50)   NOT NULL
);

INSERT INTO silver.Proyeccion_Sueldos
(EMPRESA, RUBRO, SUB_RUBRO, DETALLE, FECHA, IMPORTE, FUENTE)
SELECT
    NULLIF(LTRIM(RTRIM(EMPRESA)), ''),
    NULLIF(LTRIM(RTRIM(RUBRO)), ''),
    NULLIF(LTRIM(RTRIM(SUB_RUBRO)), ''),
    NULLIF(LTRIM(RTRIM(DETALLE)), ''),
    COALESCE(
        TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(FECHA)),''), 103),
        TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(FECHA)),''), 101),
        TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(FECHA)),''), 23)
    ),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(IMPORTE)),''),'.',''), ',', '.')),
    'PROYECCION_SUELDOS'
FROM bronze.Proyeccion_Sueldos_Raw;

---------------------------------------------------------------
-- 10) SALDOS BANCARIOS (SALDOS_BANCARIOS.csv)
---------------------------------------------------------------
PRINT '10) SALDOS BANCARIOS - BRONZE';
IF OBJECT_ID('bronze.Saldos_Bancarios_Raw','U') IS NOT NULL DROP TABLE bronze.Saldos_Bancarios_Raw;

CREATE TABLE bronze.Saldos_Bancarios_Raw
(
    FECHA         VARCHAR(20),
    EMPRESA       VARCHAR(50),
    BANCO         VARCHAR(50),
    SALDO_INICIAL VARCHAR(50),
    INVERSIONES   VARCHAR(50),
    DESCUBIERTOS  VARCHAR(50)
);

SET @sql = N'
BULK INSERT bronze.Saldos_Bancarios_Raw
FROM ''' + REPLACE(@RutaCSV,'''','''''') + N'SALDOS_BANCARIOS.csv''
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '','',
    ROWTERMINATOR   = ''0x0a'',
    CODEPAGE        = ''65001'',
    TABLOCK
);';
EXEC sys.sp_executesql @sql;

PRINT '10) SALDOS BANCARIOS - SILVER';
IF OBJECT_ID('silver.Saldos_Bancarios','U') IS NOT NULL DROP TABLE silver.Saldos_Bancarios;

CREATE TABLE silver.Saldos_Bancarios
(
    ID            INT IDENTITY(1,1) PRIMARY KEY,
    FECHA         DATE,
    EMPRESA       VARCHAR(50),
    BANCO         VARCHAR(50),
    SALDO_INICIAL DECIMAL(18,2),
    INVERSIONES   DECIMAL(18,2),
    DESCUBIERTOS  DECIMAL(18,2),
    FUENTE        VARCHAR(30)
);

INSERT INTO silver.Saldos_Bancarios
(FECHA, EMPRESA, BANCO, SALDO_INICIAL, INVERSIONES, DESCUBIERTOS, FUENTE)
SELECT
    COALESCE(TRY_CONVERT(date, LTRIM(RTRIM(FECHA)), 23), TRY_CONVERT(date, LTRIM(RTRIM(FECHA)), 103), TRY_CONVERT(date, LTRIM(RTRIM(FECHA)), 101)),
    LTRIM(RTRIM(EMPRESA)),
    LTRIM(RTRIM(BANCO)),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(SALDO_INICIAL)),''),'.',''), ',', '.')),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(INVERSIONES)),''),'.',''), ',', '.')),
    TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(DESCUBIERTOS)),''),'.',''), ',', '.')),
    'SALDOS_BANCARIOS'
FROM bronze.Saldos_Bancarios_Raw;

---------------------------------------------------------------
-- VALIDACIONES
---------------------------------------------------------------
PRINT 'VALIDACIONES - COUNTS SILVER';

SELECT 'silver.Cheques_Recibidos'    AS Tabla, COUNT(*) AS Filas FROM silver.Cheques_Recibidos
UNION ALL SELECT 'silver.Cheques_Emitidos',     COUNT(*) FROM silver.Cheques_Emitidos
UNION ALL SELECT 'silver.Ctacte_Proveedor',     COUNT(*) FROM silver.Ctacte_Proveedor
UNION ALL SELECT 'silver.EXTRA_RATIOS',         COUNT(*) FROM silver.EXTRA_RATIOS
UNION ALL SELECT 'silver.Importaciones_Aduana', COUNT(*) FROM silver.Importaciones_Aduana
UNION ALL SELECT 'silver.Impuestos',            COUNT(*) FROM silver.Impuestos
UNION ALL SELECT 'silver.Intereses',            COUNT(*) FROM silver.Intereses
UNION ALL SELECT 'silver.Proyeccion_Cobranzas', COUNT(*) FROM silver.Proyeccion_Cobranzas
UNION ALL SELECT 'silver.Proyeccion_Sueldos',   COUNT(*) FROM silver.Proyeccion_Sueldos
UNION ALL SELECT 'silver.Saldos_Bancarios',     COUNT(*) FROM silver.Saldos_Bancarios;

PRINT 'FIN - Script ejecutado.';
