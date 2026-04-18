USE OlistDWH;
GO

-- Normalizes a text value for consistent storage and grouping:
--   1. Strips leading/trailing whitespace
--   2. Converts to uppercase
--   3. Removes Brazilian Portuguese diacritics (á->A, ã->A, ç->C, etc.)
--   4. Handles common encoding artefacts from ISO-8859-1 -> UTF-8 misreads
--
-- Used in cleansed SPs for city and state fields to prevent accent variants
-- (e.g. 'São Paulo', 'Sao Paulo', 'SAO PAULO') from producing separate groups
-- in mart aggregations.
CREATE OR ALTER FUNCTION dbo.fn_normalize_text (@input NVARCHAR(MAX))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @input IS NULL RETURN NULL;

    DECLARE @result NVARCHAR(MAX) = UPPER(TRIM(@input));

    SET @result = REPLACE(@result, 'Á', 'A'); SET @result = REPLACE(@result, 'Â', 'A');
    SET @result = REPLACE(@result, 'Ã', 'A'); SET @result = REPLACE(@result, 'À', 'A');
    SET @result = REPLACE(@result, 'É', 'E'); SET @result = REPLACE(@result, 'Ê', 'E');
    SET @result = REPLACE(@result, 'Í', 'I');
    SET @result = REPLACE(@result, 'Ó', 'O'); SET @result = REPLACE(@result, 'Ô', 'O');
    SET @result = REPLACE(@result, 'Õ', 'O');
    SET @result = REPLACE(@result, 'Ú', 'U');
    SET @result = REPLACE(@result, 'Ç', 'C');

    SET @result = REPLACE(@result, 'Ä', 'A');
    SET @result = REPLACE(@result, 'Ë', 'E');
    SET @result = REPLACE(@result, 'Ü', 'U');

    RETURN @result;
END;
GO
