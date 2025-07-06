from datetime import datetime, timedelta, time
import pandas as pd

def allocate_register_salesfloor(emp_requirements, work_status_df, greeter_assignment):
    # Collect register needed count
    shift_req_df=emp_requirements[['From_Time', 'To_Time', 'Reg_Up_Needed', 'Reg_Down_Needed']]

    # Collect emp's work status
    work_status_df= work_status_df[work_status_df['Working Flag']==1][['Name', 'Start_time', 'End_time', 'Working Flag', 'Remaining_hours_left']]


    # Allocate Registers
    register_allocation= shift_req_df.copy()
    register_allocation['Register Up']= None
    register_allocation['Register Down']= None
    register_allocation['SF Up']= None
    register_allocation['SF Down']= None

    # Prepare intial variables
    current_RUs= []
    current_RDs= []
    current_SFUs= []
    current_SFDs=[]
    store_open_time= shift_req_df['From_Time'].min() #time(9, 00) 
    store_close_time= shift_req_df['From_Time'].max()
    time_delta= timedelta(minutes=30)

    curr_time= store_open_time

    while (curr_time < store_close_time):
        curr_time_plus30= (datetime.combine(datetime.today(), curr_time) + time_delta).time()
        print("---------------------------------------------------------------------------------------------")
        print(f"Allocating registers between {curr_time} to {curr_time_plus30} ...")

        # Find how many total registers are needed, skip if none
        needed_RU_count, needed_RD_count= shift_req_df[(shift_req_df['From_Time']== curr_time) & (shift_req_df['To_Time']== curr_time_plus30)][['Reg_Up_Needed','Reg_Down_Needed']].values[0]
        if (needed_RU_count + needed_RD_count == 0):
            print("No Registers Needed")
            curr_time= curr_time_plus30
            continue

        # Working Employees
        all_working_emp_list= work_status_df[(work_status_df['Start_time']== curr_time) & (work_status_df['End_time']== curr_time_plus30)]['Name'].to_list()
        print("Total # Emp Working:", len(all_working_emp_list))
        print("Total Emp Working:", all_working_emp_list)
        
        # Greeters allocated already for the current shift
        greeter_filtered= greeter_assignment[(greeter_assignment['From_Time']== curr_time) & (greeter_assignment['To_Time']== curr_time_plus30)]
        greeter_emp_list= [greeter_filtered['Upstairs Greeter'].values[0],greeter_filtered['Downstairs Greeter'].values[0]]
        while None in greeter_emp_list:
            greeter_emp_list.remove(None)
        print("Greeters Count:", len(greeter_emp_list))    
        print("Current Greeters:", greeter_emp_list)

        # Baseed on the workring and greeter list, calculate the retined register list
        retained_RUs=[]
        retained_RDs= []

        for emp in current_RUs:
            if (emp not in all_working_emp_list):
                # EMP shift got over
                continue
            if (emp in greeter_emp_list):
                # EMP moved to greeter
                continue
            else:
                retained_RUs.append(emp)
        
        for emp in current_RDs:
            if (emp not in all_working_emp_list):
                # EMP shift got over
                continue
            if (emp in greeter_emp_list):
                # EMP moved to greeter
                continue
            else:
                retained_RDs.append(emp)

        # Find how many new registers are needed
        RU_retained_count= len(retained_RUs)
        RD_retained_count= len(retained_RDs)
        new_RU_needed_count= needed_RU_count-RU_retained_count
        new_RD_needed_count= needed_RD_count-RD_retained_count

        # Create the priority table. Use the same table to assign both RU and RD
        priority_table= work_status_df[(work_status_df['Start_time']== curr_time) & (work_status_df['End_time']== curr_time_plus30) & (~work_status_df['Name'].isin(retained_RUs + retained_RDs + greeter_emp_list))]    #(work_status_df['Name'] not in retained_RUs+retained_RDs+greeter_emp_list)]
        priority_table['RU_Priority'] = priority_table['Remaining_hours_left'].rank(method='min', ascending=False).astype(int)
        priority_table.sort_values(by='RU_Priority', inplace=True)
        priority_table.reset_index(drop=True, inplace=True)

        # Pick the new registers for both RU and RD
        print('RU_retained_count', RU_retained_count)
        print('RD_retained_count', RD_retained_count)
        print('retained_RUs', retained_RUs)
        print('retained_RDs', retained_RDs)
        print('new_RU_needed_count', new_RU_needed_count)
        print('new_RD_needed_count', new_RD_needed_count)
        new_RU_assigned= priority_table['Name'][:new_RU_needed_count].tolist()
        new_RD_assigned= priority_table['Name'][new_RU_needed_count: new_RU_needed_count+new_RD_needed_count].tolist()
        print('new_RU_assigned', new_RU_assigned)
        print('new_RD_assigned', new_RD_assigned)

        # Assign the remaining to salesfloor
        unassigned_count= len(all_working_emp_list)- len(greeter_emp_list)- RU_retained_count- RD_retained_count- len(new_RU_assigned)- len(new_RD_assigned)
        print("unassigned_count", unassigned_count)
        

            # Retrive the current Salesfloors to allocate them to same up/down place
        salesfloor_up_assigned=[]
        salesfloor_dwn_assigned=[]
        max_salesfloor_up_assigned_count= int(unassigned_count/2)
        max_salesfloor_dwn_assigned_count= unassigned_count-max_salesfloor_up_assigned_count
        salesfloor_up_assigned_count=0
        salesfloor_dwn_assigned_count=0
        

        if(unassigned_count<=0):
            salesfloor_up_assigned_count=0
            salesfloor_up_assigned= []
            salesfloor_dwn_assigned_count=0
            salesfloor_dwn_assigned= []
        else:
            unassigned_emp_list=priority_table['Name'].tolist()[len(priority_table)-unassigned_count:]
                # If someone from the unassigned emp list was in SF Up in the last shift, let him/her be in the same SF Up until the max count is not exceeded
            remaining_unassigned_emp_list=[]
            for unassigned_emp in unassigned_emp_list:
                # If in SF Up in the last shift and SF Up space left for current shift
                if((unassigned_emp in current_SFUs) and (salesfloor_up_assigned_count<= max_salesfloor_up_assigned_count)):
                    salesfloor_up_assigned.append(unassigned_emp)
                    salesfloor_up_assigned_count+=1
                # If in SF Down in the last shift and SF Down space left for current shift
                elif((unassigned_emp in current_SFDs) and (salesfloor_dwn_assigned_count<= max_salesfloor_dwn_assigned_count)):
                    salesfloor_dwn_assigned.append(unassigned_emp)
                    salesfloor_dwn_assigned_count+=1
                else:
                    remaining_unassigned_emp_list.append(unassigned_emp)

            salesfloor_up_assigned= salesfloor_up_assigned+remaining_unassigned_emp_list[:max_salesfloor_up_assigned_count-salesfloor_up_assigned_count]
            salesfloor_dwn_assigned= salesfloor_dwn_assigned+remaining_unassigned_emp_list[max_salesfloor_up_assigned_count-salesfloor_up_assigned_count:]

        print('salesfloor_up_assigned_count', max_salesfloor_up_assigned_count)
        print('salesfloor_up_assigned', salesfloor_up_assigned)
        print('salesfloor_dwn_assigned_count', max_salesfloor_dwn_assigned_count)
        print('salesfloor_dwn_assigned', salesfloor_dwn_assigned)
        
        # Update the registers in the final table
        for i, row in register_allocation.iterrows():
            if row['From_Time'] == curr_time and row['To_Time'] == curr_time_plus30:
                # Assign new register up (RU) employees
                register_allocation.at[i, 'Register Up'] = retained_RUs+new_RU_assigned
                # Assign new register down (RD) employees
                register_allocation.at[i, 'Register Down'] = retained_RDs+new_RD_assigned
                # Assign new SF up employees
                register_allocation.at[i, 'SF Up'] = salesfloor_up_assigned
                # Assign new SF down employees
                register_allocation.at[i, 'SF Down'] = salesfloor_dwn_assigned
        
        # Updates few variables for the next iteration
        curr_time= curr_time_plus30
        current_RUs= retained_RUs+ new_RU_assigned
        current_RDs= retained_RDs+ new_RD_assigned
        current_SFUs= salesfloor_up_assigned
        current_SFDs= salesfloor_dwn_assigned

    return register_allocation