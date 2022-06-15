#!/usr/bin/env python

""" This module provides a jQuery like class for python: Chain.  
"""

__author__ =  'Hao Deng <denghao8888@gmail.com>'
__date__ = '1 Sept 2012'

import operator

def _lam(str):
	locals, globals = _caller_env()
	if str.strip() == '':
		return lambda x : x 

	body = 'lambda _ : ' + str
	return eval (body, globals, locals)

def _binary_compose(f, g):
	""" return composition of functions f and g. """
	def c(x):
		return g(f(x))
	return c

def _compose(funcs):
	""" return composition of functions. """
	return reduce(_binary_compose, funcs, lambda x : x )

def _str2func(f):
	if type(f) == str:
		f1 = _compose( map(_lam, f.split('|'))) 
	elif callable(f):
		f1 = f
	else:
		assert False, 'f is not a function'
	return f1

def _chainit(x):
	if isinstance(x, Chain):
		return x 
	else:
		try:
			return Chain(x)
		except TypeError :
			return x 


class Chain(list):
	""" A wrapper of list that will call callable on each element, and return
	results in a Chain if possible.

	"""

	def each(self, f):
		""" apply f on each elemment, and return the results in a Chain.

		f can be a callable,

		>>> Chain([1,2,3]).each( lambda x : x * 2 )
		[2, 4, 6]

		or string contains underscore(_). In that case, it's converted to a
		lambda function like this:
			lambda _ : {f as a string} 
		For example, if f is '_ + 1', then the function is lambda _ : _ + 1 

		>>> Chain([1,2,3]).each(' _ + 1')
		[2, 3, 4]

		f can also contain pipeline( | ) character, and it acts like unix 
		pipeline, so ' _ + 1 | _ * 2 ' will add 1 to _ first, then double it.

		>>> Chain([1,2,3]).each(' _ + 1 | _ * 2')
		[4, 6, 8]

		If you need | in your lambda, use lambda directly
		>>> Chain([1,2,3]).each(lambda _ :  _ | 2 ) 
		[3, 2, 3]

		Empty string is treated as identity function that does nothing to the 
		element. 

		>>> Chain([1,2,3]).each('')
		[1, 2, 3]


		"""
		f1 = _str2func(f)
		return Chain( map(f1, self))

	def _each_method(self, ident):
		def each_method(*args, **kwargs):
			return self.each(lambda x : getattr(x, ident)(*args, **kwargs)) 
		return each_method

	def _each_func(self, f ):
		assert callable(f)
		def mapped(*args, **kwargs):
			return  self.each( (lambda x : f(x, *args, **kwargs))) 
		return mapped 

	def _each_attr(self, ident):
		return self.each( lambda x : getattr(x, ident) )
	
	def __getattr__(self, ident):
		""" if ident is an attribute of the first element, return that 
		attribute of each element in a Chain.
		if ident is a method or function name, return a method or function 
		that will call each element.

		>>> class Foo:
		...     def __init__(self, a): self.a = a
		... 	def one(self): return 1 
		... 	def plus(self, x): return self.a + x 
		... 
		>>> foos = Chain([Foo("a"), Foo("b")])

		Attribute of elemments will be proxied:
		>>> foos.a
		['a', 'b']

		Method of element will be called with each element as self.
		>>> foos.one()
		[1, 1]

		This applies to method with more than one arguments.
		>>> foos.plus('_bar')
		['a_bar', 'b_bar']

		If an identifier is neither an attribute nor a method name, it will 
		be treated as a function, and called on each element with that element
		as the first argument.
		>>> def inc(x): return x + 1
		>>> Chain([1,2,3]).inc() 
		[2, 3, 4]
		>>> def add(x, y): return x + y 
		>>> Chain([1,2,3]).add(2)
		[3, 4, 5]

		The return type is Chain,
		>>> type(foos.a) == Chain 
		True

		so calls can be chained. 
		>>> foos.one().inc().add(2)
		[4, 4]

		Getting any attribute on empty Chain will return an empty Chain.
		>>> Chain([]).whatever
		[]


		"""
		if len(self) == 0:
			return self
		attr = getattr(self[0], ident, None)
		if attr:
			# ident is an attribute 
			if not callable( attr):
				return self._each_attr(ident)
			else:
				return self._each_method(ident)
		# ident is a func in caller
		else:
			func = _find_in_caller(ident)
			if func:
				return self._each_func(func)
			
		raise AttributeError, "Chain has not attribute %s" % ident
	
	def joined(self, sep):
		""" return sep.join(self).

		>>> Chain('abc').joined('|')
		'a|b|c'

		"""
		return sep.join(self)
		
	def filter(self, predicate):
		""" filter chain by predicate.
		predicate -- has the same mean as `each` method's argument f. 

		>> Chain([1,2,3,4,5]).filter(' _%2 == 0 ')
		[2, 4]

		"""
		f1 = _str2func(predicate)
		return Chain(filter( f1, self))

	def find(self, predicate):
		""" find the first element in self that satisfy predicate. 
		predicate -- has the same mean as `each` method's argument f. 

		>> Chain([1,2,3,4,5]).find(' _%2 == 0 ')
		[2]

		"""
		f = _str2func(predicate)
		for element in self:
			if f(element):
				return element 
		return Chain([])
	
	def reduce(self, binary_func, *args):
		""" return reduce(binary_func, self, *args).

		>>> from operator import mul
		>>> Chain([2,3,4]).reduce(mul)
		24

		""" 
		return _chainit(reduce(binary_func, self, *args))

	def concat(self):
		""" concat a list of list of something, and return a list of something.

		>>> Chain( [[1,2], [3,4,5]]).concat()
		[1, 2, 3, 4, 5]

		"""	
		return _chainit(self.reduce(operator.add, []))

	def applied(self, f, return_chain = True, *args, **kwargs):
		""" return f(self, *args, **kwargs), and make the result as a Chain
		if possible.

		return_chain -- if False, will not convert result to Chain. 

		>>> Chain([(1,2)]).applied(dict, return_chain = False)
		{1: 2}

		>>> Chain([1,1,2]).applied(set)
		[1, 2]

		"""
		r = f(self, *args, **kwargs)
		if return_chain:
			r = _chainit(r)
		return r 

def _caller_env():
	from inspect import getouterframes, currentframe
	frames = getouterframes( currentframe()) 
	for frame, file, line_no, func_name, code_context, index in frames:
		# when running from bpython, __file__ is not defined. 
		__file = globals().get('__file__') or 'chain.py'
		if file == __file:
			continue
		return  frame.f_locals, frame.f_globals

def _get_from_dicts(key, *dicts, **kwargs):
	predicate = kwargs.get('predicate', lambda _: True )
	for d in dicts:
		r = d.get(key)
		if r and predicate(r):
			return r
	raise KeyError, key
					
def _find_in_caller(ident):
	""" find ident in caller's locals() first, then globals()
	caller is the frame that just above current __file__

	"""

	l, g = _caller_env()
	return _get_from_dicts(ident, l, g, __builtins__, predicate = callable)
	
if __name__ == '__main__':
	import doctest
	doctest.testmod()	


