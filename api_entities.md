### Проектирование

Витрина, содержащая информацию о выплатах курьерам.

| Поле | Сущность | Таблица слоя DDS | Необходима загрузка через API | Источник | Поле источника |
|------|----------|------------------|-------------------------------|----------|----------------|
|id|н/п|н/п|н/п|
|courier_id|Курьер|dm_couriers|Да|couriers|_id|
|courier_name|Курьер|dm_couriers|Да|couriers|name|
|settlement_year|Время|dm_timestamps|Нет|
|settlement_month|Время|dm_timestamps|Нет|
|orders_count|Продажи|fct_product_sales|Нет|
|orders_total_sum|Продажи|fct_product_sales|Нет|
|rate_avg|Доставки|fct_deliveries|Да|deliveries|rate|
|order_processing_fee|Продажи|fct_product_sales|Нет|
|courier_order_sum|Доставки|fct_deliveries|Да|deliveries|sum|
|courier_tips_sum|Доставки|fct_deliveries|Да|deliveries|tip_sum|
|courier_reward_sum|Доставки|fct_deliveries|Да|deliveries|sum, tip_sum|