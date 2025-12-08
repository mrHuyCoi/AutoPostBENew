INSERT INTO colors (id, name, hex_code, user_id, created_at, updated_at)
VALUES
    (gen_random_uuid(), 'Đen', '#000000', '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW()),
    (gen_random_uuid(), 'Trắng', '#FFFFFF', '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW()),
    (gen_random_uuid(), 'Xanh dương', '#0000FF', '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW()),
    (gen_random_uuid(), 'Đỏ', '#FF0000', '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW());
INSERT INTO device_storage (id, device_info_id, capacity, user_id, created_at, updated_at)
VALUES
    (gen_random_uuid(), '009583fc-2458-4546-a6db-0aa8e56db977', 64,  '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW()),
    (gen_random_uuid(), '009583fc-2458-4546-a6db-0aa8e56db977', 128, '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW()),
    (gen_random_uuid(), '009583fc-2458-4546-a6db-0aa8e56db977', 256, '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW()),
    (gen_random_uuid(), '009583fc-2458-4546-a6db-0aa8e56db977', 512, '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW());
INSERT INTO device_colors (id, device_info_id, color_id, user_id, created_at, updated_at)
VALUES
    (gen_random_uuid(), '009583fc-2458-4546-a6db-0aa8e56db977', 'd30f02ac-16da-46b5-ae69-888ed8e47633', '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW()),
    (gen_random_uuid(), '009583fc-2458-4546-a6db-0aa8e56db977', 'd30f02ac-16da-46b5-ae69-888ed8e47633', '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW()),
    (gen_random_uuid(), '009583fc-2458-4546-a6db-0aa8e56db977', 'd30f02ac-16da-46b5-ae69-888ed8e47633', '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW());
INSERT INTO user_devices (
    id, user_id, device_info_id, color_id, device_storage_id,
    product_code, warranty, device_condition, device_type,
    battery_condition, price, wholesale_price, inventory, notes,
    created_at, updated_at
)
VALUES
(
    gen_random_uuid(),
    '6b9498af-e150-4a24-86cc-8c49ca595e0b',
    '009583fc-2458-4546-a6db-0aa8e56db977',
    'd30f02ac-16da-46b5-ae69-888ed8e47633',
    'dd8dbe42-8595-44ef-8428-3a77029d8f35',
    'SP000001',
    '12 tháng',
    '99%',
    'Máy cũ',
    'Pin 95%',
    10500000,
    9500000,
    3,
    'Máy còn rất mới, tặng kèm sạc nhanh.',
    NOW(),
    NOW()
);

INSERT INTO device_storage (id, device_info_id, capacity, user_id, created_at, updated_at)
VALUES
    (gen_random_uuid(), '009583fc-2458-4546-a6db-0aa8e56db977', 64,  '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW()),
    (gen_random_uuid(), 'ef19b2de-e4f1-48cd-b4f2-058c963cdf7b', 128, '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW()),
    (gen_random_uuid(), 'd39f9ddb-294c-4230-8ff0-1956c7a52735', 256, '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW()),
    (gen_random_uuid(), '642fa6cc-b5fb-465d-bf14-2a8e16c77dae', 512, '6b9498af-e150-4a24-86cc-8c49ca595e0b', NOW(), NOW());

