CREATE CONSTRAINT idx1 IF NOT EXISTS ON (a:Farmer) ASSERT a.id IS UNIQUE;
CREATE CONSTRAINT idx2 IF NOT EXISTS ON (a:Grainbatch) ASSERT a.id IS UNIQUE;
CREATE CONSTRAINT idx3 IF NOT EXISTS ON (a:Processor) ASSERT a.id IS UNIQUE;
CREATE CONSTRAINT idx4 IF NOT EXISTS ON (a:Flourbatch) ASSERT a.id IS UNIQUE;
CREATE CONSTRAINT idx5 IF NOT EXISTS ON (a:Bakery) ASSERT a.id IS UNIQUE;
CREATE CONSTRAINT idx6 IF NOT EXISTS ON (a:Breadbatch) ASSERT a.id IS UNIQUE;
CREATE CONSTRAINT idx7 IF NOT EXISTS ON (a:Machinebatch) ASSERT a.id IS UNIQUE;
CREATE CONSTRAINT idx8 IF NOT EXISTS ON (a:Breadmachine) ASSERT a.id IS UNIQUE;
CREATE CONSTRAINT idx9 IF NOT EXISTS ON (a:Customer) ASSERT a.id IS UNIQUE;
CREATE CONSTRAINT idx10 IF NOT EXISTS ON (a:Minute) ASSERT a.id IS UNIQUE;
CREATE CONSTRAINT idx11 IF NOT EXISTS ON (a:Purchase) ASSERT a.id IS UNIQUE;
CREATE CONSTRAINT idx12 IF NOT EXISTS ON (a:Second) ASSERT a.id IS UNIQUE;