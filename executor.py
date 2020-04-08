import subprocess as sp
tokens=['341249','348929','356865','408065','779521','1510401']
processes=[]
for token in tokens:
    p=sp.Popen(['python3.7','./test_3_algo_together.py',token])
    print(str(token) +"  :  " +str(p.pid))
    processes.append(p)