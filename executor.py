import subprocess as sp
tokens=[]
processes=[]
for token in tokens:
    p=sp.popen(['python','test_3_algo_together',token])
    processes.append(p)

for process in processes:
    process.kill()