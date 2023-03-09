# Database-Management-System

Overall Layered Architecture

개요 
disk based B+ TREE는 disk 상에서 동작하는 b+ tree이다. b+ tree는 record의 효율적인 
insert, find, delete를 통해 정렬된 데이터를 표현하기 위한 트리자료구조의 일종이다.
<img src="/image/image1.png" width="700" height="370">

![image]("/image/image1.png")


가장 초기의 B+tree는 크게 두 개의 layer로 구성되어있다. Bpt manager와 File manager이
다. Bpt manager는 B+ Tree operation 에 해당하는 insert, delete, find 작업을 수행한다. File manager 는 File open, write, read 와 같은 file 관련 작업을 수행한다. 즉, Data file
에 대한 접근은 File manager를 통해서만 가능한 것이다. 아래에서는 데이터 파일의 구성
요소, Bpt manager와 File manager의 동작 방식을 보다 세부적으로 다룰 것이다. What's inside the data file?
데이터 파일은 4096 byte의 페이지들로 구성되어있다. 페이지는 header page, internal 
page, leaf page, free page 총 4개의 종류이다. 
header page : 데이터 파일에 가장 앞에 존재하며 metadata를 가지고 있다. metadata는 
Free page number, Root page number, Number of pages로 이루어져있다.
internal page: internal record를 저장하는 페이지이다. internal record는 leaf record의 위
치를 알려주는 역할을 한다. 
leaf page: 실질적으로 leaf record를 가지고 있는 페이지이다. free page: leaf page, internal page가 될 수 있는 공백의 페이지이다.

<img src="/image/image2.png" width="700" height="370">

HOW Bpt manager WORKS
● insert
find함수를 통해 해당 key를 가진 record가 존재하는지 판단한다. record가 존재할 경우, value만 업데이트 한다. 그렇지 않을 경우, 새로운 record를 insert 해야 하는데 아래의 3가
지 상황에 알맞게 동작해야한다.
1. 트리가 존재하지 않는 경우
2. 트리가 존재하고 insert될 공간이 있는 경우
3. 트리가 존재하고 insert될 공간이 없는 경우
1의 경우, 새로운 트리를 만든다. 2의 경우, leaf 노드에 record를 insert 해준다. 3의 경우, leaf 노드에 record를 insert 해주고 split 해준다. 그 후 internal 노드에 해당 
record를 insert해주고 마찬가지로 노드가 가득 찼을 경우, split 해준다. 이 과정은 root 노
드까지 일어난다. ● delete
root에서 시작하여 key가 속한 leaf page를 찾는다. leaf page에서 key를 제거한다. leaf 
page의 key가 하나도 남지 않는 경우만 neighbor page와 merge가 일어난다. 다만 
neighbor page에 key가 가득 차 있는 경우, neighbor page의 capacity가 초과될 수도 있
기에 merge가 일어나지 않는다. merge가 일어났다면 leaf page 부모로부터 삭제된 페이지
를 가리키는 page_num을 삭제한다. 이러한 delete가 root까지 전파될 수 있다. 아래의 링크는 b+ tree에서 insert와 delete가 어떻게 일어나는지 시각적으로 보여준다. https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html