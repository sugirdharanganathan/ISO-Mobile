-- ================================
-- tank_details table + seed data
-- ================================

-- 1. Create table if it doesn't exist
CREATE TABLE IF NOT EXISTS tank_details (
    id INT AUTO_INCREMENT PRIMARY KEY,
    tank_id INT NOT NULL,
    tank_number VARCHAR(50) NOT NULL UNIQUE,
    status VARCHAR(50),
    mfgr VARCHAR(100),
    date_mfg DATE,
    pv_code VARCHAR(50),
    un_iso_code VARCHAR(50),
    capacity_l INT,
    mawp FLOAT,
    design_temperature FLOAT,
    tare_weight_kg INT,
    mgw_kg INT,
    mpl_kg INT,
    size VARCHAR(100),
    pump_type VARCHAR(50),
    vesmat VARCHAR(50),
    gross_kg INT,
    net_kg INT,
    color_body_frame VARCHAR(50),
    working_pressure FLOAT,
    cabinet_type VARCHAR(50),
    frame_type VARCHAR(50),
    remark VARCHAR(255),
    lease VARCHAR(50),
    created_by VARCHAR(50),
    updated_by VARCHAR(50)
);

-- 2. Seed rows (idempotent: only inserts if tank_number not present)

-- Tank 1
INSERT INTO tank_details (
    tank_id, tank_number, status, mfgr, date_mfg, pv_code, un_iso_code,
    capacity_l, mawp, design_temperature, tare_weight_kg, mgw_kg, mpl_kg,
    size, pump_type, vesmat, gross_kg, net_kg, color_body_frame,
    working_pressure, cabinet_type, frame_type, remark, lease, created_by, updated_by
)
SELECT
    1               AS tank_id,
    'SMXU 8880704'  AS tank_number,
    'active'        AS status,
    'SZHF'          AS mfgr,
    '2015-10-01'    AS date_mfg,
    'T-75 / 22K7'   AS pv_code,
    'GB150 / IMDG'  AS un_iso_code,
    20700           AS capacity_l,
    21.4            AS mawp,
    -40.0           AS design_temperature,
    11400           AS tare_weight_kg,
    34000           AS mgw_kg,
    22600           AS mpl_kg,
    '6058 × 2438 × 2591 mm' AS size,
    'Yes'           AS pump_type,
    'Standard'      AS vesmat,
    34000           AS gross_kg,
    22600           AS net_kg,
    'White'         AS color_body_frame,
    21.4            AS working_pressure,
    '-'             AS cabinet_type,
    'Frame T-2'     AS frame_type,
    NULL            AS remark,
    'No'            AS lease,
    'admin'         AS created_by,
    'admin'         AS updated_by
FROM DUAL
WHERE NOT EXISTS (
    SELECT 1 FROM tank_details WHERE tank_number = 'SMXU 8880704'
);

-- Tank 2
INSERT INTO tank_details (
    tank_id, tank_number, status, mfgr, date_mfg, pv_code, un_iso_code,
    capacity_l, mawp, design_temperature, tare_weight_kg, mgw_kg, mpl_kg,
    size, pump_type, vesmat, gross_kg, net_kg, color_body_frame,
    working_pressure, cabinet_type, frame_type, remark, lease, created_by, updated_by
)
SELECT
    2               AS tank_id,
    'SMAU 8888493'  AS tank_number,
    'active'        AS status,
    'SZHF'          AS mfgr,
    '2016-02-01'    AS date_mfg,
    'T-75 / 22K7'   AS pv_code,
    'GB150 / IMDG'  AS un_iso_code,
    20700           AS capacity_l,
    21.4            AS mawp,
    -40.0           AS design_temperature,
    11400           AS tare_weight_kg,
    34000           AS mgw_kg,
    22600           AS mpl_kg,
    '6058 × 2438 × 2591 mm' AS size,
    'Yes'           AS pump_type,
    'Standard'      AS vesmat,
    34000           AS gross_kg,
    22600           AS net_kg,
    'White'         AS color_body_frame,
    21.4            AS working_pressure,
    '-'             AS cabinet_type,
    'Frame T-2'     AS frame_type,
    NULL            AS remark,
    'No'            AS lease,
    'admin'         AS created_by,
    'admin'         AS updated_by
FROM DUAL
WHERE NOT EXISTS (
    SELECT 1 FROM tank_details WHERE tank_number = 'SMAU 8888493'
);

-- Tank 3
INSERT INTO tank_details (
    tank_id, tank_number, status, mfgr, date_mfg, pv_code, un_iso_code,
    capacity_l, mawp, design_temperature, tare_weight_kg, mgw_kg, mpl_kg,
    size, pump_type, vesmat, gross_kg, net_kg, color_body_frame,
    working_pressure, cabinet_type, frame_type, remark, lease, created_by, updated_by
)
SELECT
    3               AS tank_id,
    'SMAU 8881110'  AS tank_number,
    'active'        AS status,
    'JBOX'          AS mfgr,
    '2016-06-01'    AS date_mfg,
    'T-75 / 22K7'   AS pv_code,
    'GB150 / IMDG'  AS un_iso_code,
    21800           AS capacity_l,
    24.0            AS mawp,
    -40.0           AS design_temperature,
    10000           AS tare_weight_kg,
    36000           AS mgw_kg,
    25940           AS mpl_kg,
    '6058 × 2438 × 2591 mm' AS size,
    'Yes'           AS pump_type,
    'Standard'      AS vesmat,
    36000           AS gross_kg,
    25940           AS net_kg,
    'White'         AS color_body_frame,
    24.0            AS working_pressure,
    'Side-1'        AS cabinet_type,
    'Frame T-1'     AS frame_type,
    NULL            AS remark,
    'No'            AS lease,
    'admin'         AS created_by, 
    'admin'         AS updated_by
FROM DUAL
WHERE NOT EXISTS (
    SELECT 1 FROM tank_details WHERE tank_number = 'SMAU 8881110'
);
