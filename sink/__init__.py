
# import omnifig as fig
# # from multiprocessing import Pool, cpu_count, Pipe, Process
# from pathos.multiprocessing import ProcessingPool as Pool
# # import dill
#
# def simple_func(x):
# 	return x + 1
#
#
# @fig.script('test-mp', description='Test multiprocessing')
# def simple_test_mp(cfg: fig.Configuration):
#
# 	inp = range(10)
#
# 	with Pool(4) as p:
# 		res = p.map(simple_func, inp)
#
# 	print(res)



from .database import FileDatabase
# from . import scripts
# from . import onestep
from . import twostep