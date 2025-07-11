-- SQL query to filter bibliographic records that are not electronic ('s') in Form of Item (008/23)
-- Polaris stores fixed fields like 008 in the MARC tag table (tag = 008), 
-- and the relevant position (23rd character, 0-based index = 22) can be extracted with SUBSTRING.

SELECT DISTINCT br.BibliographicRecordID AS RecordID
FROM BibliographicRecords br
LEFT JOIN BibliographicTags bt WITH (NOLOCK) 
  ON br.BibliographicRecordID = bt.BibliographicRecordID
LEFT JOIN BibliographicSubFields bfs WITH (NOLOCK) 
  ON bt.BibliographicTagID = bfs.BibliographicTagID
WHERE br.MARCDescCatalogingForm = 'a'          -- Only cataloged records (not serials or temp)
  AND br.MARCBibType != 'a'                    -- Exclude serials
  AND bt.TagNumber = '008'                     -- Fixed field tag
  AND SUBSTRING(bfs.Data, 23, 1) NOT IN ('s', 'o')  -- Exclude Form of Item codes for electronic ('s') and online ('o')
