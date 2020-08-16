----Составьте запрос для вывода топ-5 категорий трат. см. таблицы Сredit.charge, Credit.category
SELECT cat.category_desc, SUM(ch.charge_amt) s
FROM Credit.category cat
LEFT JOIN Credit.charge ch ON ch.category_no = cat.category_no
GROUP BY cat.category_desc
ORDER BY s DESC
LIMIT 5

----Составьте запрос для вывода топ-10 поставщиков Credit.Provider внутри каждой категории трат Credit.category
SELECT t.category_desc,
	    t.provider_name
	    , t.r
	    , t.s
FROM (
	SELECT prov.category_desc,
			 prov.provider_name,
	       rank() over (PARTITION BY prov.category_desc ORDER BY prov.s DESC) AS r,
	       prov.s
	FROM (
		SELECT cat.category_desc, pr.provider_name, SUM(ch.charge_amt) s
		FROM Credit.category cat
		LEFT JOIN Credit.charge ch ON ch.category_no = cat.category_no
		LEFT JOIN Credit.provider pr ON pr.provider_no = ch.provider_no
		GROUP BY cat.category_desc, pr.provider_name
	) prov
	GROUP BY prov.category_desc, prov.provider_name, prov.s -- не понял, почему без этого не работает
) t
WHERE t.r <= 10
ORDER BY t.category_desc, t.r

---Составьте запрос для вывода всех компаний (Credit.corporation) и размера среднего платежа по кредиту (Credit.payment). Результат сортировать по количеству рабочих (Credit.member) и размеру среднего платежа
WITH corps AS (
	SELECT distinct cor.corp_name,
			 AVG(p.payment_amt) over (PARTITION BY cor.corp_no) avg_pay,
			 COUNT(*) over (PARTITION BY cor.corp_no) cnt_members
	FROM Credit.corporation cor
	LEFT JOIN Credit.member mem ON mem.corp_no = cor.corp_no
	LEFT JOIN Credit.payment p ON p.member_no = mem.member_no
)

SELECT c.corp_name,
		 c.avg_pay
FROM corps c
ORDER BY c.cnt_members DESC, c.avg_pay DESC
