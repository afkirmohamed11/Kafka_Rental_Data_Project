-- 1. Basic statistics for numerical columns
SELECT 
    AVG(CAST(Prix AS DOUBLE)) AS avg_price,
    MIN(CAST(Prix AS DOUBLE)) AS min_price,
    MAX(CAST(Prix AS DOUBLE)) AS max_price,
    AVG(CAST(Surface AS DOUBLE)) AS avg_surface,
    AVG(CAST(Nombre_De_Pieces AS DOUBLE)) AS avg_rooms,
    AVG(CAST(Nombre_De_Chambres AS DOUBLE)) AS avg_bedrooms,
    AVG(CAST(Nombre_De_Salles_De_Bain AS DOUBLE)) AS avg_bathrooms
FROM "kafka_rental_data_table"."my_kafka_project_bucket";

-- 2. Distribution of property types
SELECT 
    CASE 
        WHEN Type_De_Bien = 1 THEN 'Apartment'
        WHEN Type_De_Bien = 0 THEN 'House'
        ELSE 'Other'
    END AS property_type,
    COUNT(*) AS count,
    CAST(COUNT(*) AS DOUBLE) * 100.0 / SUM(COUNT(*)) OVER() AS percentage
FROM "kafka_rental_data_table"."my_kafka_project_bucket"
GROUP BY Type_De_Bien
ORDER BY count DESC;

-- 3. Average price by city
SELECT 
    AVG(CASE WHEN Ville_agadir THEN Prix END) AS Avg_Price_Agadir,
    AVG(CASE WHEN Ville_bouskoura THEN Prix END) AS Avg_Price_Bouskoura,
    AVG(CASE WHEN Ville_casablanca THEN Prix END) AS Avg_Price_Casablanca,
    AVG(CASE WHEN Ville_dar_bouazza THEN Prix END) AS Avg_Price_Dar_Bouazza,
    AVG(CASE WHEN Ville_kenitra THEN Prix END) AS Avg_Price_Kenitra,
    AVG(CASE WHEN Ville_marrakech THEN Prix END) AS Avg_Price_Marrakech,
    AVG(CASE WHEN Ville_mohammedia THEN Prix END) AS Avg_Price_Mohammedia,
    AVG(CASE WHEN Ville_rabat THEN Prix END) AS Avg_Price_Rabat,
    AVG(CASE WHEN Ville_sale THEN Prix END) AS Avg_Price_Sale,
    AVG(CASE WHEN Ville_tanger THEN Prix END) AS Avg_Price_Tanger
FROM "kafka_rental_data_table"."my_kafka_project_bucket"


-- 4. Properties with high-end amenities
SELECT 
    COUNT(*) AS luxury_properties,
    CAST(COUNT(*) AS DOUBLE) * 100.0 / (SELECT COUNT(*) FROM "kafka_rental_data_table"."my_kafka_project_bucket") AS percentage
FROM "kafka_rental_data_table"."my_kafka_project_bucket"
WHERE Piscine = TRUE AND Ascenseur = TRUE AND Climatisation = TRUE
      AND Securite = TRUE AND Jardin = TRUE;

-- 5. Average price by number of rooms
SELECT 
    Nombre_De_Pieces,
    AVG(CAST(Prix AS DOUBLE)) AS avg_price,
    COUNT(*) AS count
FROM "kafka_rental_data_table"."my_kafka_project_bucket"
GROUP BY Nombre_De_Pieces
ORDER BY Nombre_De_Pieces;

-- 6. Most common rental state
SELECT 
    Etat_De_Location,
    COUNT(*) AS count,
    CAST(COUNT(*) AS DOUBLE) * 100.0 / (SELECT COUNT(*) FROM "kafka_rental_data_table"."my_kafka_project_bucket") AS percentage
FROM "kafka_rental_data_table"."my_kafka_project_bucket"
GROUP BY Etat_De_Location
ORDER BY count DESC;

-- 7. Properties with outdoor spaces
SELECT 
    CASE 
        WHEN Jardin = TRUE AND Terrasse = TRUE THEN 'With Garden and Terrace'
        WHEN Jardin = TRUE THEN 'With Garden Only'
        WHEN Terrasse = TRUE THEN 'With Terrace Only'
        ELSE 'No Outdoor Space'
    END AS outdoor_space,
    COUNT(*) AS count,
    AVG(CAST(Prix AS DOUBLE)) AS avg_price
FROM "kafka_rental_data_table"."my_kafka_project_bucket"
GROUP BY 
    CASE 
        WHEN Jardin = TRUE AND Terrasse = TRUE THEN 'With Garden and Terrace'
        WHEN Jardin = TRUE THEN 'With Garden Only'
        WHEN Terrasse = TRUE THEN 'With Terrace Only'
        ELSE 'No Outdoor Space'
    END
ORDER BY count DESC;

-- 8. Price range distribution
SELECT 
    CASE 
        WHEN CAST(Prix AS DOUBLE) < 1000 THEN 'Under 1000'
        WHEN CAST(Prix AS DOUBLE) BETWEEN 1000 AND 2000 THEN '1000-2000'
        WHEN CAST(Prix AS DOUBLE) BETWEEN 2000 AND 3000 THEN '2000-3000'
        WHEN CAST(Prix AS DOUBLE) BETWEEN 3000 AND 4000 THEN '3000-4000'
        WHEN CAST(Prix AS DOUBLE) BETWEEN 4000 AND 5000 THEN '4000-5000'
        ELSE 'Over 5000'
    END AS price_range,
    COUNT(*) AS count,
    CAST(COUNT(*) AS DOUBLE) * 100.0 / (SELECT COUNT(*) FROM "kafka_rental_data_table"."my_kafka_project_bucket") AS percentage
FROM "kafka_rental_data_table"."my_kafka_project_bucket"
GROUP BY 1
ORDER BY count DESC;

-- 9. Furnished vs Unfurnished properties
SELECT 
    CASE WHEN Meuble = TRUE THEN 'Furnished' ELSE 'Unfurnished' END AS furnishing,
    COUNT(*) AS count,
    AVG(CAST(Prix AS DOUBLE)) AS avg_price
FROM "kafka_rental_data_table"."my_kafka_project_bucket"
GROUP BY Meuble
ORDER BY count DESC;