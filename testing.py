from anytree import Node, RenderTree, find, Resolver
import os

file1 = {"id": 0, "datanodes": [], "size": 32, "metadata": "grrrrrrrrr"}
file2 = {"id": 1, "datanodes": [], "size": 12, "metadata": "dhdjhsgdhjas"}
file3 = {"id": 2, "datanodes": [], "size": 12, "metadata": "dhdjhsgdhjas"}

root = Node("root")
pics = Node('pics', parent=root)
docs = Node('docs', parent=root)
sum14 = Node('summer 2014', parent=pics)
sum14_1 = Node('pic1', parent=sum14, file=file1)
sum14_2 = Node('pic2', parent=sum14, file=file2)
report = Node('report', parent=docs, file=file3)

print(os.path.basename('/dsf/sdf/zhopa'))

# path = 'summer 2014/pic1'
# r = Resolver('name')
# print(r.get(pics, path))
#
# #
#
# path = '/root/pics/summer 2014'
# r = Resolver('name')
# resp = r.get(root, path)
# new_node = Node('new file', parent=resp, file={"id": 3})
#
# new_node = Node('new file2', parent=resp, file={"id": 4})
#
#
# path = '/root/pics/summer 2014/pic2'
# resp = r.get(root, path)
# file = resp.file
# resp.parent = None
#
# resp.parent = docs
#
# print(RenderTree(root))

#node = find(root, lambda x: r.get(root, x) == path)
#print(node)

#print(list(node.children))


# class A:
#     def __init__(self):
#         self.a = 0
#
#     def incr_a(self):
#         self.a += 1
#
#
# a = A()
#
# def test():
#     a.incr_a()
#
# print(a.a)
# test()
# print(a.a)
# test()
# print(a.a)