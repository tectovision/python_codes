#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 23 14:20:26 2023

@author: garcia
"""

import garcia_fct as ga
from geodezyx import *                   # Import the GeodeZYX modules
from geodezyx.externlib import *         # Import the external modules
from geodezyx.megalib.megalib import *   # Import the legacy modules names


dire_res_sat = '/home/garcia/wrk_garcia/CASCADIA_NET/ANA/'
file=ga.find_txt_gen(dire_res_sat,recursive_search=True,severe=True,string ='08.06_solve_2_ambfix.log')
filtered_file = [path for path in file if '/ITERA/' not in path]
filtered_file = [path for path in filtered_file if '/FAIL/' not in path]
fortran_bug_stk = [] 
amb_bug_stk=[]
new_rows_cln =[]
nuevas_filas_2 =[]
nuevas_filas_3 =[]

i=0
for path in filtered_file:
    
    F = open(path,'r')
    Lines = F.readlines() 
    F.close()

    for line in Lines:
        if "forrtl: severe" in line:
            fortran_bug_stk.append(path[43:51])

        

    HeadDate = utils.grep(path,' **** ERROR: oneway not found',
                          only_first_occur=True,line_number=True)
    
    
    
    if  HeadDate[0] and HeadDate[1]:
        print('ok')
        header = (Lines[HeadDate[0]+7].split())
        header_2 = (Lines[HeadDate[0]+3].split())
        head = HeadDate[0]+8
        head_2 = HeadDate[0]+4
        Downfooter = utils.grep(path,' program-finish: add_ambfixcon    error-number:',
                              only_first_occur=True,line_number=True)
    
        Downfooter_2 = utils.grep(path,'**** ERROR: all entries of -slvambig-',
                              only_first_occur=True,line_number=True)
    
    
        foot = abs(Downfooter[0] - len(Lines))
        foot_2= abs(Downfooter_2[0] - len(Lines)-1)
        
        df_amb = pd.read_csv(path,skiprows=(head),header=None,skipfooter=foot,
                         delim_whitespace = True, engine='python',
                         names=header)
        
        df_amb.drop(columns=df_amb.columns[:2],axis=1, inplace=True)
        #xx=df_amb.reset_index(drop=True)
        
        df_amb_2 = pd.read_csv(path,skiprows=(head_2),header=None,skipfooter=foot_2,
                         delim_whitespace = True, engine='python',
                         names=header_2)
        
        df_amb_2.drop(columns=df_amb_2.columns[:2],axis=1, inplace=True)
        
        
        # Obtener los valores de sta1 y sta2 de df_amb_2
        sat1 = df_amb_2['isat1'][0]
        sat2 = df_amb_2['isat2'][0]
        amb0 = df_amb_2['t0'][0]
        amb0 = df_amb_2['t1'][0]
        # Filtrar df_amb_2 para obtener las filas donde AMB(i)%nusta coincide con sta1 o sta2
        df_amb_f = df_amb[(df_amb['AMB(i)%nusat'] == sat1) | (df_amb['AMB(i)%nusat'] == sat2)]

        df_amb_f_2 = df_amb_f[(df_amb_f['AMB(i)%t0'] > df_amb_2['t0'][0]) |
                              (df_amb_f['AMB(i)%t1'] < df_amb_2['t1'][0])]
        
        df_amb_f_2=df_amb_f_2.reset_index(drop=True)
        
        nf0 =' DEL'
        t0_min = round(min(df_amb['AMB(i)%t0']),5)
        t1_max = round(max(df_amb['AMB(i)%t1']),5)
        sta_amb=(df_amb['AMB(i)%nusta'].unique()).tolist()
        date = path[43:51]+'_0002'
        commet = 'CASCADIA_AMB'
        for sta in sta_amb:    
            nfila = nf0+' '+str(t0_min)+' '+str(t1_max)+' '+str(sta)+' '+' '+'0'+'   '+date+' '+commet
            new_rows_cln.append(nfila) 
        0
        #print(df_amb_2)
        for k in df_amb_f_2.index:
            t0=round(df_amb_f_2['AMB(i)%t0'][k],5)
            t1=round(df_amb_f_2['AMB(i)%t1'][k],5)
            sta = df_amb_f_2['AMB(i)%nusta'][k]
            sat = df_amb_f_2['AMB(i)%nusat'][k]
            nfila_2 = nf0+' '+str(t0)+' '+str(t1)+' '+str(sta)+' '+' '+str(sat)+'   '+date+' '+commet
            nuevas_filas_2.append(nfila_2) 
        
        
        #print(df_amb)
   
         
fortran_bug_stk_r = [s.replace('_', '') for s in fortran_bug_stk]

print(' the list of the days to repeat due cluster bugs is:',fortran_bug_stk_r)
print(' rhe number of job to run will be: '+str(len(fortran_bug_stk_r)))
ambiguities_bug_stk=[]
date_amb_stk=[]
for row in new_rows_cln:
    # Dividir la fila en palabras
    words = row.split()
    date_amb0 =words[-2][:8]
    date_amb_stk.append(date_amb0)
    date_amb=date_amb0.replace('_','')
    ambiguities_bug_stk.append(date_amb)

# using list comprehension to remove duplicated from list
ambiguities_bug_stk_r=[]
[ambiguities_bug_stk_r.append(x) for x in ambiguities_bug_stk  if x not in ambiguities_bug_stk_r]


# date_amb_stk_r = list(set(date_amb_stk))

date_amb_stk_r=[]
[date_amb_stk_r.append(x) for x in date_amb_stk  if x not in date_amb_stk_r]

date_amb_stk_r = sorted(date_amb_stk_r)

print(' the list of the days to repeat due ambiguities bugs is:',date_amb_stk_r)
print(' rhe number of job to run will be: '+str(len(date_amb_stk_r)))
#%%

dire_res_sat = '/home/garcia/wrk_garcia/CASCADIA_NET/ANA/'
file=ga.find_txt_gen(dire_res_sat,recursive_search=True,severe=True,string ='08.13_clean_set_limits.log')
filtered_file = [path for path in file if '/ITERA/' not in path]
filtered_file = [path for path in filtered_file if '/FAIL/' not in path]

residual_high_stk=[]
i=0
for path in filtered_file:
    
    F = open(path,'r')
    Lines = F.readlines() 
    F.close()

    for line in Lines:
        if "**** error : number of records (amb_file) : 40001" in line:
            residual_high_stk.append(path[43:51])
#residual_high_stk=['2015_009','2015_015','2015_051']
#residual_high_stk=['2015_015']
date_residual_high_stk = [fila.replace("_", "") for fila in residual_high_stk]

#residual_high_stk=['2015_009']
nuevas_filas_3 = []

for yyyy_doy in residual_high_stk:
    #dire_res_sat = '/home/garcia/wrk_garcia/CASCADIA_NET_0/ANA/2015_009/PROT/'
    dire_res_sat = '/home/garcia/wrk_garcia/CASCADIA_NET/ANA/'+yyyy_doy+'/PROT/'
    [res,t0,t1]=ga.ana_res_epos_itera(dire_res_sat)
    nf0 =' DEL'
    date = dire_res_sat[43:51]+'_0002'
    for index,tup in enumerate(res):
        sta = tup[0]
        if int(tup[1])<10:
            sat = 'G0'+tup[1]
        else:
            sat = 'G'+tup[1]
        nfila_2 = nf0+' '+t0+' '+t1+' '+str(sta)+' '+' '+str(sat)+'   '+date+' '+commet+'_HIGH_RES'
        nuevas_filas_3.append(nfila_2)
print(nuevas_filas_3)

#%%  to update the CLN FILE based on ambiguities issues based on new_rows
input_file_path = "/home/garcia/wrk_garcia/CONTROL/CNT_CASCADIA_NET/CLN_GLOBAL_CASCADIA_082223_231209_161715"
#ga.edit_cln_epos(new_rows_cln, input_file_path)
#ga.edit_cln_epos(nuevas_filas_2, input_file_path)
ga.edit_cln_epos(nuevas_filas_3, input_file_path)

#%% To run EPOS in a cluster mode 
import os

### path of the config file used in EPOS
config_opera = '/home/garcia/wrk_garcia/CONFIG_NET_CASCADIA_082223.xml'
epos_start = 'perl_epos /dsk/igs2/SOFT_EPOS8_ROUTINE/IGS/SOFT_EPOS8_BIN/SCRIPTS/epos8_start.pl'
cmd_line_stk=[]
#fortran_bug_stk_r=fortran_bug_stk_r[0]

for day in fortran_bug_stk_r:
    kommand_slr = epos_start+ " -cfg " + config_opera + " -beg_day " + day + ' -rod save ; /bin/rm -rf /dsk1/tmp/garcia/\*'
    cmd_line_stk.append(kommand_slr)


amd_line_stk=[]
for day in ambiguities_bug_stk_r:
    #kommand_gnss_amb =  "epos_start -cfg " + config_opera + " -beg_day " + day + ' -rod save ; /bin/rm -rf /dsk1/tmp/garcia/\*'
    kommand_gnss_amb = epos_start+ " -cfg " + config_opera + " -beg_day " + day + ' -rod save ; /bin/rm -rf /dsk1/tmp/garcia/\*'
    amd_line_stk.append(kommand_gnss_amb)


res_line_stk=[]
for day in date_residual_high_stk:
    #kommand_gnss_amb =  "epos_start -cfg " + config_opera + " -beg_day " + day + ' -rod save ; /bin/rm -rf /dsk1/tmp/garcia/\*'
    kommand_gnss_res = epos_start+ " -cfg " + config_opera + " -beg_day " + day + ' -rod save ; /bin/rm -rf /dsk1/tmp/garcia/\*'
    res_line_stk.append(kommand_gnss_res)



#cmd_line_stk0=['perl_epos /dsk/igs2/SOFT_EPOS8_ROUTINE/IGS/SOFT_EPOS8_BIN/SCRIPTS/epos8_start.pl -cfg /home/garcia/wrk_garcia/CONFIG_NET_CASCADIA_082223.xml -beg_day 2020007 -rod save ; /bin/rm -rf /dsk1/tmp/garcia/\\*']

#cmd_line_stk=[cmd_line_stk[0]]

#amd_line_stk=['epos_start -cfg /home/garcia/wrk_garcia/CONFIG_NET_CASCADIA_082223.xml -beg_day 2020002 -rod save ; /bin/rm -rf /dsk1/tmp/garcia/\\*']


dir_path_ana='/home/garcia/wrk_garcia/CASCADIA_NET_0/ANA/'
dir_path_cln='/home/garcia/wrk_garcia/CASCADIA_NET_0/CLN/'
dir_path=[dir_path_ana,dir_path_cln]
options=['fortran','ambiguities','residual','other']
options=['residual']
for opt in options:
    try:
        if opt =='fortran':
            ver='01'
            for folder in dir_path:
                for file_name in fortran_bug_stk:
                    file_path = os.path.join(folder,file_name)
                
                    new_name = f'{file_name}_{ver}'
                
                    new_path= os.path.join(folder,new_name)
                    print(new_path)
                    try:
                        os.rename(file_path,new_path)
                        print(f'folder {file_name} was renamed to {new_name}')
                        print()
                    
                        print(f'folder {file_name} was renamed to {new_name}')
                    
                    except OSError as err:
                        print(f'error to rename {file_name}: {err}')
            operational.cluster_GFZ_run(cmd_line_stk,
                                          bunch_on_off=True,
                                          bunch_job_nbr=len(cmd_line_stk), 
                                          bunch_wait_time=10,
                                          bj_check_on_off=True, 
                                          bj_check_mini_nbr=55, 
                                          bj_check_wait_time=10, 
                                          bj_check_user="auto")
        elif opt =='ambiguities':
            ver='01'
            for folder in dir_path:
                for file_name in date_amb_stk_r:
                    file_path = os.path.join(folder,file_name)
                
                    new_name = f'{file_name}_{ver}'
                
                    new_path= os.path.join(folder,new_name)
                    print(new_path)
                    try:
                        os.rename(file_path,new_path)
                        print(f'folder {file_name} was renamed to {new_name}')
                        print()
                    
                        print(f'folder {file_name} was renamed to {new_name}')
                    
                    except OSError as err:
                        print(f'error to rename {file_name}: {err}')
    
    
    
    
            operational.cluster_GFZ_run(amd_line_stk,
                                        bunch_on_off=True,
                                        bunch_job_nbr=len(amd_line_stk), 
                                        bunch_wait_time=10,
                                        bj_check_on_off=True, 
                                        bj_check_mini_nbr=55, 
                                        bj_check_wait_time=10, 
                                        bj_check_user="auto")
        elif opt =='residual':
            ver='01_RES'
            for folder in dir_path:
                for file_name in date_residual_high_stk:
                    file_path = os.path.join(folder,file_name)
                
                    new_name = f'{file_name}_{ver}'
                
                    new_path= os.path.join(folder,new_name)
                    print(new_path)
                    try:
                        os.rename(file_path,new_path)
                        print(f'folder {file_name} was renamed to {new_name}')
                        print()
                    
                        print(f'folder {file_name} was renamed to {new_name}')
                    
                    except OSError as err:
                        print(f'error to rename {file_name}: {err}')
    
    
    
    
            operational.cluster_GFZ_run(res_line_stk,
                                        bunch_on_off=True,
                                        bunch_job_nbr=len(res_line_stk), 
                                        bunch_wait_time=10,
                                        bj_check_on_off=True, 
                                        bj_check_mini_nbr=55, 
                                        bj_check_wait_time=10, 
                                        bj_check_user="auto")

        elif opt =='other':
            # inser manually 
            def generate_date_list(year, day_start, day_end):
                date_list = []
            
                for day in range(day_start, day_end + 1):
                    # Ensure that day is formatted with leading zeros if necessary
                    day_str = str(day).zfill(3)
            
                    # Create the date string in the "year_day" format
                    date_string = f"{year}{day_str}"
            
                    date_list.append(date_string)
            
                return date_list
    
            # Example usage:
            year = 2020
            day_start =8
            day_end = 8
            files_epos = generate_date_list(year, day_start, day_end)
            print(files_epos)
            
            files_epos_stk=[]
            for day in files_epos:
                #kommand_gnss ="epos_start -cfg " + config_opera + " -beg_day " + day + ' -rod save ; /bin/rm -rf /dsk1/tmp/garcia/\*'
                kommand_gnss = epos_start + " -cfg " + config_opera + " -beg_day " + day + ' -rod save ; /bin/rm -rf /dsk1/tmp/garcia/\*'
                
                files_epos_stk.append(kommand_gnss)
            
            
            operational.cluster_GFZ_run(files_epos_stk,
                                        bunch_on_off=True,
                                        bunch_job_nbr=len(files_epos_stk), 
                                        bunch_wait_time=10,
                                        bj_check_on_off=True, 
                                        bj_check_mini_nbr=55, 
                                        bj_check_wait_time=10, 
                                        bj_check_user="auto")
        else:
            print('Nothing to do')
    except Exception as err:
        print("going further")

#%%

import subprocess
import pandas as pd
# Especifica el directorio que deseas listar
directorio = dir_path_ana
ruta_archivo = "/home/garcia/wrk_garcia/CASCADIA_NET/status_processing"

# Ejecuta el comando ls -lh y redirige la salida a un archivo CSV
comando = f"ls -lh {directorio} | awk '{{print $5 \",\" $9}}' > {ruta_archivo}"
subprocess.run(comando, shell=True)
comando = f"ls -lh {directorio} | awk '{{print $5 \",\" $9}}'"
resultado = subprocess.check_output(comando, shell=True, text=True)

# Divide el resultado en líneas
lineas = resultado.strip().split('\n')
day_status=lineas[1:-1]
s_stk=[]
d_stk=[]
for day_size in day_status:
    ds=day_size.split(',')
    s_stk.append(ds[0])
    d_stk.append(ds[1])

# Crear un DataFrame a partir de las listas
df_status = pd.DataFrame({'size': s_stk, 'file': d_stk})

# Mostrar el DataFrame
print(df_status)

# Filtrar las filas donde "Columna_1" sea igual a 6
df_fil = df_status[df_status['size'] == str(6)]

# Mostrar el DataFrame filtrado
print(df_fil)


# import subprocess

# def run_week(year, day_of_year):
#     # Formatear la entrada como YYYYDDD
#     input_str = f"{year}{day_of_year:03d}"
    
#     # Comando que deseas ejecutar (reemplaza la ruta con la correcta)
#     week_command = "/dsk/igs2/SOFT_EPOS8_ROUTINE/IGS/SOFT_EPOS8_BIN_TOOLS/INTL64/xmjd_tabelle2.out"

#     try:
#         # Ejecutar el comando y pasar la entrada formateada
#         output = subprocess.check_output([week_command, input_str], text=True)
#         return output
#     except subprocess.CalledProcessError as e:
#         return f"Error: {e}"

# # Ejemplo de uso
# year = 2018
# doy = 5
# result = run_week(year, doy)
# print(result)

# for ch in result:
#     print(ch)
    
# #%%

# import subprocess

# def run_semisys_help(batch, stations):
#     # Comando para llamar a Perl con el script semisysExport.pm y los argumentos
#     command = [
#         "perl",
#         "/dsk/igs2/SOFT_EPOS8_ROUTINE/IGS/SOFT_EPOS8_BIN_ODC/SCRIPTS/semisysExport.pm",
#         "-batch", batch,
#         "-station", stations
#     ]

#     try:
#         # Ejecutar el comando
#         output = subprocess.check_output(command, text=True)
#         return output
#     except subprocess.CalledProcessError as e:
#         return f"Error: {e}"

# # Ejemplo de uso
# batch_number = "1102"
# station_names = "ALBH, POTS"
# result = run_semisys_help(batch_number, station_names)
# print(result)

# for line in result.splitlines():  # to dive the result as list of lines
#     # Procesar cada línea según sea necesario
#     print(line)
