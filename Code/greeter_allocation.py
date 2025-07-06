

def assign_priority(df, greeter_shift_done_dict):
    # Initialize the priority column
    df['Priority'] = 0  # Initialize the Priority column with zeros
    
    # Group by Start_time and End_time
    for (start, end), group in df.groupby(['Start_time', 'End_time']):
        responsible_people = df[(df['Start_time'] == start) & 
                                         (df['End_time'] == end)]# & 
                                         #(df['Responsibility'] == 'Greeter')]
        
        if not responsible_people.empty:
            for index, row in responsible_people.iterrows():
                greeter = row['Name']
                if greeter in group['Name'].values:
                    df.loc[(df['Start_time'] == start) & (df['End_time'] == end) & (df['Name'] == greeter), 'Priority'] = 1
            
            remaining_group = group[~group['Name'].isin(responsible_people['Name'])]
            non_zero_hours_group = remaining_group[remaining_group['Remaining_hours_left'] > 0].sort_values(by='Remaining_hours_left', ascending=True)
            
            # Assign dense ranking starting from 2 for non-zero hours employees
            if not non_zero_hours_group.empty:
                non_zero_hours_group['Priority'] = non_zero_hours_group['Remaining_hours_left'].rank(method='dense', ascending=True).astype(int) + 1
                df.loc[non_zero_hours_group.index, 'Priority'] = non_zero_hours_group['Priority']
            
            # Employees with 0 hours remaining
            zero_hours_group = remaining_group[remaining_group['Remaining_hours_left'] == 0]
            if not zero_hours_group.empty:
                max_priority = non_zero_hours_group['Priority'].max() if not non_zero_hours_group.empty else 1
                zero_hours_group['Priority'] = max_priority + 1
                df.loc[zero_hours_group.index, 'Priority'] = zero_hours_group['Priority']
        else:
            group_with_non_zero_hours = group[group['Remaining_hours_left'] > 0].sort_values(by='Remaining_hours_left', ascending=True)
            group_with_zero_hours = group[group['Remaining_hours_left'] == 0]
            if not group_with_non_zero_hours.empty:
                group_with_non_zero_hours['Priority'] = group_with_non_zero_hours['Remaining_hours_left'].rank(method='dense', ascending=True).astype(int)
                df.loc[group_with_non_zero_hours.index, 'Priority'] = group_with_non_zero_hours['Priority']
            if not group_with_zero_hours.empty:
                max_priority = group_with_non_zero_hours['Priority'].max() if not group_with_non_zero_hours.empty else 1
                group_with_zero_hours['Priority'] = max_priority + 1
                df.loc[group_with_zero_hours.index, 'Priority'] = group_with_zero_hours['Priority']

    return df  # Return the modified DataFrame with the Priority column

def allocate_greeter(greeter_priority_df, emp_requirements):

    # Create greeter shift completed dictionary
    unique_names = greeter_priority_df['Name'].unique()
    greeter_shift_done_dict = {name: 0 for name in unique_names} # Create a dictionary with names as keys and 0 as the initial value
    
    # Assign priorities before processing time periods
    greeter_priority_df = assign_priority(greeter_priority_df, greeter_shift_done_dict)

    # Collected required # greeters
    greeter_assignment=emp_requirements[['From_Time','To_Time','Greeter_Down_Needed','Greeter_Up_Needed']]
    time_periods = greeter_assignment.drop_duplicates()

    for idx, period in time_periods.iterrows():
        # Extract needed counts for upstairs and downstairs greeters
        up_needed = int(period['Greeter_Up_Needed'])
        down_needed = int(period['Greeter_Down_Needed'])

        current_period_data = greeter_priority_df[
            (greeter_priority_df['Start_time'] == period['From_Time']) &
            (greeter_priority_df['End_time'] == period['To_Time'])
        ]

        # Debug: Check current period data
        print(f"Current Period: {period['From_Time']} to {period['To_Time']}")
        print("Current Period Data:\n", current_period_data)

        # Sort employees by priority
        sorted_employees = current_period_data.sort_values('Priority')

        # Assign upstairs greeters based on needed count
        upstairs_greeters = sorted_employees.head(up_needed)['Name'].tolist()
        print("Upstairs Greeters Assigned:", upstairs_greeters)

        # Filter out already assigned upstairs greeters for downstairs assignments
        remaining_employees = sorted_employees[~sorted_employees['Name'].isin(upstairs_greeters)]

        # Assign downstairs greeters based on needed count
        downstairs_greeters = remaining_employees.head(down_needed)['Name'].tolist() if down_needed > 0 else []
        print("Downstairs Greeters Assigned:", downstairs_greeters)
        print("-----------------------------------------------------------------------------")

        # Update the greeter_assignment DataFrame with assigned greeters
        greeter_assignment.at[idx, 'Upstairs Greeter'] = upstairs_greeters[0] if upstairs_greeters else None
        greeter_assignment.at[idx, 'Downstairs Greeter'] = downstairs_greeters[0] if downstairs_greeters else None

        # Update shift counts for assigned employees
        for employee in upstairs_greeters + downstairs_greeters:
            if employee:  # Only update for actual assignments
                greeter_shift_done_dict[employee] += 1

        # Update the priorities for assigned greeters
        for employee in upstairs_greeters + downstairs_greeters:
            if employee:  # Only update for actual assignments
                greeter_priority_df.loc[greeter_priority_df['Name'] == employee, 'Priority'] = greeter_priority_df['Priority'].max() + 1

    return greeter_assignment, greeter_shift_done_dict
