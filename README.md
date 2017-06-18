# DRChord: A Distributed Hash Table with dRuby

## Overview
DRChord is one of the distributed hash table (DHT).

It is based structured Peer-to-Peer architecture.

## Install

```
bundle install
```

## Usage

### Start node01

```
% bundle exec ruby main.rb
I, [2016-06-04T19:03:05.260044 #81577]  INFO -- : dRuby server start - druby://127.0.0.1:3000
I, [2016-06-04T19:03:05.260117 #81577]  INFO -- : Ctrl-C to shutdown node
I, [2016-06-04T19:03:05.261176 #81577]  INFO -- : Press the enter key to print node info...
```

### Start node02

```
% bundle exec ruby main.rb -p 4000 -e 127.0.0.1:3000
I, [2016-06-04T19:03:45.940079 #82426]  INFO -- : dRuby server start - druby://127.0.0.1:4000
I, [2016-06-04T19:03:45.940152 #82426]  INFO -- : Ctrl-C to shutdown node
I, [2016-06-04T19:03:45.940294 #82426]  INFO -- : Press the enter key to print node info...
```

### Start DHT shell

```
% bundle exec ruby shell.rb -n 127.0.0.1:3000

> help
Command list:
  status
  put
  delete
  help
  exit

> put hello world
true

> get hello
Value: world

> delete hello
Key & Value deleted. - (hello)
```

## Licence

[MIT](https://github.com/shiftky/drchord/blob/master/LICENCE)
