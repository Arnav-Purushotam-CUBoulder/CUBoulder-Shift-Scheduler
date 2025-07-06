-- Create transformed schema
CREATE SCHEMA transformed;

-- Create table emp_req
drop table transformed.emp_req;

CREATE TABLE transformed.emp_req (
    From_Time VARCHAR(100) NOT NULL,
    To_Time VARCHAR(100) NOT NULL,
    Reg_Up_Needed INT NOT NULL,
    Reg_Down_Needed INT NOT NULL,
    Greeter_Up_Needed INT NOT NULL,
    Greeter_Down_Needed INT NOT NULL,
    Min_Total_Emp_Needed INT NOT NULL,
    Total_Avl_Emp FLOAT NOT NULL,
    Availability_Check_Flag BOOLEAN NOT NULL
);

select * from transformed.emp_req;

-- Create table work_status
drop table transformed.work_status;

CREATE TABLE transformed.work_status (
    Name VARCHAR(255) NOT NULL,
    Start_time VARCHAR(100) NOT NULL,
    End_time VARCHAR(100) NOT NULL,
    Working_Flag INT NOT NULL,
    Remaining_hours_left FLOAT NOT NULL
);

select * from transformed.work_status;



-- Create scheduled schema
CREATE SCHEMA scheduled;

-- Create table emp_req
drop table scheduled.final_allocation;

CREATE TABLE scheduled.final_allocation (
    From_Time VARCHAR(100) NOT NULL,
    To_Time VARCHAR(100) NOT NULL,
    Greeter_Down_Needed INT NOT NULL,
    Greeter_Up_Needed INT NOT NULL,
    Upstairs_Greeter VARCHAR(255),        -- Can be NULL
    Downstairs_Greeter VARCHAR(255),      -- Can be NULL
    Reg_Up_Needed INT NOT NULL,
    Reg_Down_Needed INT NOT NULL,
    Register_Up VARCHAR(255),            -- Array of strings
    Register_Down VARCHAR(255),          -- Array of strings
    SF_Up VARCHAR(255),                  -- Array of strings
    SF_Down VARCHAR(255)                 -- Array of strings
);
