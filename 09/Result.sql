/*1. Используя данные о переписи и предварительной переписи получить:
Расхождения в значениях между предварительной и финальной переписью
*/

-- с джойном
SELECT	pre.region_name AS 'Регион',
		pre.population_type AS 'Тип населения',
        pre.sex_type AS 'Пол',
        fin.count - pre.count AS 'Разница м/у финальной и предварительной переписью'
FROM	pre_census AS pre
JOIN	final_census AS fin 
	ON pre.region_name = fin.region_name
    AND pre.population_type = fin.population_type
    AND pre.sex_type = fin.sex_type
ORDER BY pre.region_name,
		pre.population_type,
        pre.sex_type
;

-- и без джойна        
SELECT	pre.region_name AS 'Регион',
		pre.population_type AS 'Тип населения',
        pre.sex_type AS 'Пол',
        fin.count - pre.count AS 'Разница м/у финальной и предварительной переписью'
FROM	pre_census AS pre,
		final_census AS fin 
WHERE   pre.region_name = fin.region_name
        AND pre.population_type = fin.population_type
        AND pre.sex_type = fin.sex_type
ORDER BY pre.region_name,
		pre.population_type,
        pre.sex_type
;

/* Посчитать по каждому региону процентное соотношение женщин и мужчин и городского и сельского населения, составить топы 3 по регионам с преобладанием тех или иных критериев(мужчин, женщин, городского, сельского населения) */

/* Процентаж по регионам мужчин и женщин*/
SELECT  men.region_name AS 'Регион',
        men.count AS 'Кол-во мужчин',
        women.count AS 'Кол-во женщин',
        men.count/(men.count + women.count) * 100 AS '% мужчин',
        women.count/(men.count + women.count) * 100 AS '% женщин'
FROM    final_census AS men,
        final_census AS women
WHERE   men.region_name = women.region_name
        AND men.population_type = 'все население'
        AND women.population_type = men.population_type
        AND men.sex_type = 'мужчины'
        AND women.sex_type = 'женщины'
ORDER BY men.region_name
;

/* ТОП3 регионов с максимальным % мужчин */
SELECT  men.region_name AS 'Регион с макс % мужчин',
        men.count AS 'Кол-во мужчин',
        women.count AS 'Кол-во женщин',
        men.count/(men.count + women.count) * 100 AS '% мужчин',
        women.count/(men.count + women.count) * 100 AS '% женщин'
FROM    final_census AS men,
        final_census AS women
WHERE   men.region_name = women.region_name
        AND men.population_type = 'все население'
        AND women.population_type = men.population_type
        AND men.sex_type = 'мужчины'
        AND women.sex_type = 'женщины'
ORDER BY men.count/(men.count + women.count) DESC
LIMIT 3
;

/* ТОП3 регионов с максимальным % женщин */
SELECT  men.region_name AS 'Регион с макс % женщин',
        men.count AS 'Кол-во мужчин',
        women.count AS 'Кол-во женщин',
        men.count/(men.count + women.count) * 100 AS '% мужчин',
        women.count/(men.count + women.count) * 100 AS '% женщин'
FROM    final_census AS men,
        final_census AS women
WHERE   men.region_name = women.region_name
        AND men.population_type = 'все население'
        AND women.population_type = men.population_type
        AND men.sex_type = 'мужчины'
        AND women.sex_type = 'женщины'
ORDER BY women.count/(men.count + women.count) DESC
LIMIT 3
;


/* Процентаж по регионам городского и сельского населения */
SELECT  city.region_name AS 'Регион',
        city.count AS 'Объем городского населения',
        village.count AS 'Объем сельского населения',
        city.count/(city.count + village.count) * 100 AS '% городского',
        village.count/(city.count + village.count) * 100 AS '% сельского'
FROM    final_census AS city,
        final_census AS village
WHERE   city.region_name = village.region_name
        AND city.sex_type = 'Всего'
        AND village.sex_type = city.sex_type
        AND city.population_type = 'городское население'
        AND village.population_type = 'сельское население'
ORDER BY city.region_name
;

/* ТОП3 регионов с максимальным % городского */
SELECT  city.region_name AS 'Регион с макс % городского населения',
        city.count AS 'Объем городского населения',
        village.count AS 'Объем сельского населения',
        city.count/(city.count + village.count) * 100 AS '% городского',
        village.count/(city.count + village.count) * 100 AS '% сельского'
FROM    final_census AS city,
        final_census AS village
WHERE   city.region_name = village.region_name
        AND city.sex_type = 'Всего'
        AND village.sex_type = city.sex_type
        AND city.population_type = 'городское население'
        AND village.population_type = 'сельское население'
ORDER BY city.count/(city.count + village.count) DESC
LIMIT 3
;

/* ТОП3 регионов с максимальным % сельского */
SELECT  city.region_name AS 'Регион с макс % сельского населения',
        city.count AS 'Объем городского населения',
        village.count AS 'Объем сельского населения',
        city.count/(city.count + village.count) * 100 AS '% городского',
        village.count/(city.count + village.count) * 100 AS '% сельского'
FROM    final_census AS city,
        final_census AS village
WHERE   city.region_name = village.region_name
        AND city.sex_type = 'Всего'
        AND village.sex_type = city.sex_type
        AND city.population_type = 'городское население'
        AND village.population_type = 'сельское население'
ORDER BY village.count/(city.count + village.count)  DESC
LIMIT 3
;


/* 2. Получить аналогичные данные по Пермскому краю и его субъектам используюя соответствующие данные. Сравнить данные полученые на этом этапе с данными из переписи(Пермский край). Сравнить пермский край со средними значениями по Российской федерации. */

/* Процентаж мужчин и женщин по регионам Пермского края  (для каждого года) */
SELECT  men.region_name AS 'Регион',
        men.date AS 'Дата',
        men.count AS 'Кол-во мужчин',
        women.count AS 'Кол-во женщин',
        men.count/(men.count + women.count) * 100 AS '% мужчин',
        women.count/(men.count + women.count) * 100 AS '% женщин'
FROM    stat_Permsky_men AS men,
        stat_Permsky_women AS women
WHERE   men.region_name = women.region_name
        AND men.date = women.date
ORDER BY men.region_name,
        men.date
;


/* Процентаж мужчин и женщин по регионам Пермского края (для 2010 года, года переписи населения) */
SELECT  men.region_name AS 'Регион',
        men.count AS 'Кол-во мужчин',
        women.count AS 'Кол-во женщин',
        men.count/(men.count + women.count) * 100 AS '% мужчин',
        women.count/(men.count + women.count) * 100 AS '% женщин'
FROM    stat_Permsky_men AS men,
        stat_Permsky_women AS women
WHERE   men.region_name = women.region_name
        AND men.date = women.date
        AND men.date = STR_TO_DATE('01.01.2010', '%d.%m.%Y')
ORDER BY men.region_name
;


/* ТОП3 регионов Пермского края с максимальным % мужчин для 2010 года*/
SELECT  men.region_name AS 'Регион с макс % мужчин',
        men.count AS 'Кол-во мужчин',
        women.count AS 'Кол-во женщин',
        men.count/(men.count + women.count) * 100 AS '% мужчин',
        women.count/(men.count + women.count) * 100 AS '% женщин'
FROM    stat_Permsky_men AS men,
        stat_Permsky_women AS women
WHERE   men.region_name = women.region_name
        AND men.date = women.date
        AND men.date = STR_TO_DATE('01.01.2010', '%d.%m.%Y')
ORDER BY men.count/(men.count + women.count) DESC
LIMIT 3
;

/* ТОП3 регионов Пермского края с максимальным % женщин для 2010 года*/
SELECT  men.region_name AS 'Регион с макс % мужчин',
        men.count AS 'Кол-во мужчин',
        women.count AS 'Кол-во женщин',
        men.count/(men.count + women.count) * 100 AS '% мужчин',
        women.count/(men.count + women.count) * 100 AS '% женщин'
FROM    stat_Permsky_men AS men,
        stat_Permsky_women AS women
WHERE   men.region_name = women.region_name
        AND men.date = women.date
        AND men.date = STR_TO_DATE('01.01.2010', '%d.%m.%Y')
ORDER BY women.count/(men.count + women.count) DESC
LIMIT 3
;


/* Процентаж городского и сельского населения по регионам Пермского края  (для каждого года) */
SELECT  city.region_name AS 'Регион',
        city.date AS 'Дата записи',
        city.count AS 'Объем городского',
        village.count AS 'Объем сельского',
        city.count/(city.count + village.count) * 100 AS '% городского',
        village.count/(city.count + village.count) * 100 AS '% сельского'
FROM    stat_Permsky_city AS city,
        stat_Permsky_village AS village
WHERE   city.region_name = village.region_name
        AND city.date = village.date
ORDER BY city.region_name,
        city.date
;

/* Процентаж мужчин и женщин по регионам Пермского края (для 2010 года, года переписи населения) */

SELECT  city.region_name AS 'Регион',
        city.count AS 'Объем городского',
        village.count AS 'Объем сельского',
        city.count/(city.count + village.count) * 100 AS '% городского',
        village.count/(city.count + village.count) * 100 AS '% сельского'
FROM    stat_Permsky_city AS city,
        stat_Permsky_village AS village
WHERE   city.region_name = village.region_name
        AND city.date = village.date
        AND city.date = STR_TO_DATE('01.01.2010', '%d.%m.%Y')
ORDER BY city.region_name
;


/* ТОП3 регионов Пермского края с максимальным % городского населения для 2010 года*/
SELECT  city.region_name AS 'Регион с макс % горожан',
        city.count AS 'Объем городского',
        village.count AS 'Объем сельского',
        city.count/(city.count + village.count) * 100 AS '% городского',
        village.count/(city.count + village.count) * 100 AS '% сельского'
FROM    stat_Permsky_city AS city,
        stat_Permsky_village AS village
WHERE   city.region_name = village.region_name
        AND city.date = village.date
        AND city.date = STR_TO_DATE('01.01.2010', '%d.%m.%Y')
ORDER BY city.count/(city.count + village.count) DESC
LIMIT 3
;

/* ТОП3 регионов Пермского края с максимальным % сельского населения для 2010 года*/
SELECT  city.region_name AS 'Регион с макс % сельчан',
        city.count AS 'Объем городского',
        village.count AS 'Объем сельского',
        city.count/(city.count + village.count) * 100 AS '% городского',
        village.count/(city.count + village.count) * 100 AS '% сельского'
FROM    stat_Permsky_city AS city,
        stat_Permsky_village AS village
WHERE   city.region_name = village.region_name
        AND city.date = village.date
        AND city.date = STR_TO_DATE('01.01.2010', '%d.%m.%Y')
ORDER BY village.count/(city.count + village.count) DESC
LIMIT 3
;


/* Сравнить данные полученые на этом этапе с данными из переписи(Пермский край) */
SELECT  city.region_name AS 'Регион',
        fin_men.count - men.count AS 'Разница мужского населения',
        fin_women.count - women.count AS 'Разница женского населения',
        fin_city.count - city.count AS 'Разница городского населения',
        fin_village.count - village.count AS 'Разница сельского населения'
FROM    stat_Permsky_men AS men,
        stat_Permsky_women AS women,
        stat_Permsky_city AS city,
        stat_Permsky_village AS village,
        final_census AS fin_men,
        final_census AS fin_women,
        final_census AS fin_city,
        final_census AS fin_village
WHERE   men.region_name = 'Пермский край'
        AND women.region_name = men.region_name
        AND city.region_name = men.region_name
        AND village.region_name = men.region_name
        
        AND city.date = STR_TO_DATE('01.01.2010', '%d.%m.%Y')
        AND village.date = city.date
        AND men.date = city.date
        AND women.date = city.date
        
        AND fin_men.region_name = men.region_name
        AND fin_women.region_name = men.region_name
        AND fin_city.region_name = men.region_name
        AND fin_village.region_name = men.region_name
        
        AND fin_men.population_type = 'все население'
        AND fin_women.population_type = fin_men.population_type
        AND fin_men.sex_type = 'мужчины'
        AND fin_women.sex_type = 'женщины'
        
        AND fin_city.sex_type = 'Всего'
        AND fin_village.sex_type = fin_city.sex_type
        AND fin_city.population_type = 'городское население'
        AND fin_village.population_type = 'сельское население'
;


/* Сравнить Пермский край со средними значениями по Российской федерации */
SELECT  city.region_name AS 'По сравнению со средним по России',
        (fin_men.count /(fin_men.count + fin_women.count) - men.count / (men.count + women.count))*100 AS 'Разница мужского населения, %',
        (fin_women.count /(fin_men.count + fin_women.count) - women.count / (men.count + women.count))*100 AS 'Разница женского населения, %',
        (fin_city.count /(fin_city.count + fin_village.count) - city.count / (city.count + village.count))*100 AS 'Разница городского населения, %',
        (fin_village.count /(fin_city.count + fin_village.count) - village.count / (city.count + village.count))*100 AS 'Разница сельского населения, %'
FROM    stat_Permsky_men AS men,
        stat_Permsky_women AS women,
        stat_Permsky_city AS city,
        stat_Permsky_village AS village,
        final_census AS fin_men,
        final_census AS fin_women,
        final_census AS fin_city,
        final_census AS fin_village
WHERE   men.region_name = 'Пермский край'
        AND women.region_name = men.region_name
        AND city.region_name = men.region_name
        AND village.region_name = men.region_name
        
        AND city.date = STR_TO_DATE('01.01.2010', '%d.%m.%Y')
        AND village.date = city.date
        AND men.date = city.date
        AND women.date = city.date
        
        AND fin_men.region_name = 'Российская Федерация'
        AND fin_women.region_name = fin_men.region_name
        AND fin_city.region_name = fin_men.region_name
        AND fin_village.region_name = fin_men.region_name
        
        AND fin_men.population_type = 'все население'
        AND fin_women.population_type = fin_men.population_type
        AND fin_men.sex_type = 'мужчины'
        AND fin_women.sex_type = 'женщины'
        
        AND fin_city.sex_type = 'Всего'
        AND fin_village.sex_type = fin_city.sex_type
        AND fin_city.population_type = 'городское население'
        AND fin_village.population_type = 'сельское население'
;




/* 3. Посчитать по Пермскому краю естественный прирост населения за каждый год.
Дополнительно сравнить прирост среди мужчин и женщин. 
Можно выполнить расчет по районам, в случае затруднений по Перми и по области. */

/* Посчитать по Пермскому краю естественный прирост населения за каждый год. */
SELECT  men.region_name AS 'Регион',
        YEAR(men_shift.date) AS 'Год',
        men_shift.count - men.count AS 'Прирост мужского населения',
        women_shift.count - women.count AS 'Прирост женского населения',
        city_shift.count - city.count AS 'Прирост городского населения',
        village_shift.count - village.count AS 'Прирост сельского населения'
FROM    stat_Permsky_men AS men,
        stat_Permsky_women AS women,
        stat_Permsky_city AS city,
        stat_Permsky_village AS village,
        
        stat_Permsky_men AS men_shift,
        stat_Permsky_women AS women_shift,
        stat_Permsky_city AS city_shift,
        stat_Permsky_village AS village_shift
WHERE   women.region_name = men.region_name
        AND city.region_name = men.region_name
        AND village.region_name = men.region_name
        
        AND men_shift.region_name = men.region_name
        AND women_shift.region_name = men.region_name
        AND city_shift.region_name = men.region_name
        AND village_shift.region_name = men.region_name
        
        AND women.date = men.date
        AND city.date = men.date
        AND village.date = men.date
        
        AND YEAR(men_shift.date) = YEAR(men.date) + 1 -- смещение на 1 год
        AND YEAR(women_shift.date) = YEAR(men.date) + 1
        AND YEAR(city_shift.date) = YEAR(men.date) + 1
        AND YEAR(village_shift.date) = YEAR(men.date) + 1
ORDER BY    men.region_name,
            YEAR(men_shift.date)
;



/* 4. (альтернативное) Используя данные по криминогенности регионов посчитать среднюю криминогенность за период и вывести топ 5 криминогенных районов */
SELECT  reg.region_name AS 'Регион',
        AVG(stat.procent) AS 'Средняя криминогенность за период'
FROM    crimanal_region AS reg,
        crimanal_stat AS stat
WHERE   reg.id_region = stat.id_region
GROUP BY reg.region_name
ORDER BY AVG(stat.procent) DESC
LIMIT 5
;