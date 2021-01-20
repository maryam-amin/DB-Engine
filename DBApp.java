package m2;

import java.awt.Dimension;
import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;
import gnu.trove.procedure.TIntProcedure;
import net.sf.jsi.Rectangle;
import net.sf.jsi.rtree.RTree;

public class DBApp {

	int n = 200;
	int ns = 15;

	public void saveObject(Object o, String path) {
		try {
			FileOutputStream fileOut = new FileOutputStream(path);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(o);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public Object loadObject(String path) {
		Object o = null;
		try {
			FileInputStream fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			o = in.readObject();
			in.close();
			fileIn.close();
		} catch (ClassNotFoundException | IOException i) {
			i.printStackTrace();
		}
		return o;
	}

	public ArrayList<String> getTableIndexed(String strTableName) {
		ArrayList<String> indices = new ArrayList<String>();
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
			String row;
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(strTableName) && data[4].equals("true")) {
					indices.add(data[1]);
				}
			}
			csvReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return indices;
	}

	public String getTableKey(String strTableName) {
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
			String row;
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(strTableName) && data[3].equals("true")) {
					csvReader.close();
					return data[1];
				}
			}
			csvReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";
	}

	public String getColType(String strTableName, String strColName) {
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
			String row;
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(strTableName) && data[1].equals(strColName)) {
					csvReader.close();
					return data[2];
				}
			}
			csvReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";
	}

	public void init() {
		try {
			Properties prop = new Properties();
			prop.load(new FileInputStream("config/DBApp.properties"));
			n = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
			ns = Integer.parseInt(prop.getProperty("NodeSize"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
	}

	@SuppressWarnings("unchecked")
	public void createBTreeIndex(String strTableName, String strColName) throws DBAppException {
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
			String row;
			String metadata = "";
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(strTableName) && data[1].equals(strColName)) {
					metadata += data[0] + "," + data[1] + "," + data[2] + "," + data[3] + ",true\n";
				} else {
					metadata += row + "\n";
				}
			}
			csvReader.close();
			FileWriter csvWriter = new FileWriter("data/metadata.csv");
			csvWriter.append(metadata);
			csvWriter.flush();
			csvWriter.close();
			String path = "data/" + strTableName + strColName + ".class";
			switch (getColType(strTableName, strColName)) {
			case "java.lang.Integer":
				saveObject(new BxTree<Integer, Integer>(), path);
				break;
			case "java.lang.String":
				saveObject(new BxTree<String, Integer>(), path);
				break;
			case "java.lang.Double":
				saveObject(new BxTree<Double, Integer>(), path);
				break;
			case "java.lang.Boolean":
				saveObject(new BxTree<Boolean, Integer>(), path);
				break;
			case "java.util.Date":
				saveObject(new BxTree<Date, Integer>(), path);
				break;
			}
			Vector<Hashtable<String, Object>> rows = new Vector<Hashtable<String, Object>>();
			int page = 0;
			String file = "data/" + strTableName + page + ".class";
			while ((new File(file)).exists()) {
				rows = (Vector<Hashtable<String, Object>>) loadObject(file);
				for (Hashtable<String, Object> h : rows) {
					switch (getColType(strTableName, strColName)) {
					case "java.lang.Integer":
						BxTree<Integer, Integer> bti = (BxTree<Integer, Integer>) loadObject(path);
						bti.insert((int) h.get(strColName), page);
						saveObject(bti, path);
						break;
					case "java.lang.String":
						BxTree<String, Integer> bts = (BxTree<String, Integer>) loadObject(path);
						bts.insert((String) h.get(strColName), page);
						saveObject(bts, path);
						break;
					case "java.lang.Double":
						BxTree<Double, Integer> btd = (BxTree<Double, Integer>) loadObject(path);
						btd.insert((Double) h.get(strColName), page);
						saveObject(btd, path);
						break;
					case "java.lang.Boolean":
						BxTree<Boolean, Integer> btb = (BxTree<Boolean, Integer>) loadObject(path);
						btb.insert((Boolean) h.get(strColName), page);
						saveObject(btb, path);
						break;
					case "java.util.Date":
						BxTree<Date, Integer> btda = (BxTree<Date, Integer>) loadObject(path);
						btda.insert((Date) h.get(strColName), page);
						saveObject(btda, path);
						break;
					}
				}
				page++;
				file = "data/" + strTableName + page + ".class";
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public void createRTreeIndex(String strTableName, String strColName) throws DBAppException {
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
			String row;
			String metadata = "";
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(strTableName) && data[1].equals(strColName)) {
					metadata += data[0] + "," + data[1] + "," + data[2] + "," + data[3] + ",true\n";
				} else {
					metadata += row + "\n";
				}
			}
			csvReader.close();
			FileWriter csvWriter = new FileWriter("data/metadata.csv");
			csvWriter.append(metadata);
			csvWriter.flush();
			csvWriter.close();
			RTree rtree = new RTree();
			rtree.init(null);
			Vector<Hashtable<String, Object>> rows = new Vector<Hashtable<String, Object>>();
			int page = 0;
			String file = "data/" + strTableName + page + ".class";
			while ((new File(file)).exists()) {
				rows = (Vector<Hashtable<String, Object>>) loadObject(file);
				for (Hashtable<String, Object> h : rows) {
					java.awt.Rectangle b = ((Polygon) h.get(strColName)).getBounds();
					Rectangle r = new Rectangle((float) b.getMinX(), (float) b.getMinY(), (float) b.getMaxX(),
							(float) b.getMaxY());
					rtree.add(r, page);
				}
				page++;
				file = "data/" + strTableName + page + ".class";
			}
			saveObject(rtree, "data/" + strTableName + strColName + ".class");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@SuppressWarnings("unchecked")
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		int page = 0;
		String file = "data/" + strTableName + page + ".class";
		Vector<Hashtable<String, Object>> rows = new Vector<Hashtable<String, Object>>();
		if (!(new File(file)).exists()) {
			rows.add(htblColNameValue);
			saveObject(rows, file);
		} else {
			while ((new File(file)).exists()) {
				rows = (Vector<Hashtable<String, Object>>)loadObject(file);
				rows.add(htblColNameValue);
				saveObject(rows, file);
				break;
			}
		}

		ArrayList<String> indices = getTableIndexed(strTableName);
		for (String s : indices) {
			String path = "data/" + strTableName + s + ".class";
			switch (getColType(strTableName, s)) {
			case "java.lang.Integer":
				BxTree<Integer, Integer> bti = (BxTree<Integer, Integer>) loadObject(path);
				bti.insert((int) htblColNameValue.get(s), page);
				saveObject(bti, path);
				break;
			case "java.lang.String":
				BxTree<String, Integer> bts = (BxTree<String, Integer>) loadObject(path);
				bts.insert((String) htblColNameValue.get(s), page);
				saveObject(bts, path);
				break;
			case "java.lang.Double":
				BxTree<Double, Integer> btd = (BxTree<Double, Integer>) loadObject(path);
				btd.insert((Double) htblColNameValue.get(s), page);
				saveObject(btd, path);
				break;
			case "java.lang.Boolean":
				BxTree<Boolean, Integer> btb = (BxTree<Boolean, Integer>) loadObject(path);
				btb.insert((Boolean) htblColNameValue.get(s), page);
				saveObject(btb, path);
				break;
			case "java.util.Date":
				BxTree<Date, Integer> btda = (BxTree<Date, Integer>) loadObject(path);
				btda.insert((Date) htblColNameValue.get(s), page);
				saveObject(btda, path);
				break;
			case "java.awt.Polygon":
				RTree rtree = (RTree) loadObject(path);
				java.awt.Rectangle b = ((Polygon) htblColNameValue.get(s)).getBounds();
				Rectangle r = new Rectangle((float) b.getMinX(), (float) b.getMinY(), (float) b.getMaxX(),
						(float) b.getMaxY());
				rtree.add(r, page);
				saveObject(rtree, path);
			}
		}
	}
	
	@SuppressWarnings({ "deprecation", "unchecked" })
	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {

		ArrayList<String> indices = getTableIndexed(strTableName);
		String key = getTableKey(strTableName);
		HashSet<Integer> toUpdate = new HashSet<Integer>();
		Object keyObject = new Object();
		Hashtable<Hashtable<String, Object>, Integer> oldRows = new Hashtable<Hashtable<String, Object>, Integer>();

		// cast key
		switch (getColType(strTableName, key)) {
		case "java.lang.Integer":
			keyObject = Integer.parseInt(strClusteringKey);
			break;
		case "java.lang.String":
			keyObject = strClusteringKey;
			break;
		case "java.lang.Double":
			keyObject = Double.parseDouble(strClusteringKey);
			break;
		case "java.lang.Boolean":
			keyObject = Boolean.parseBoolean(strClusteringKey);
			break;
		case "java.util.Date":
			keyObject = new Date(Date.parse(strClusteringKey));
			break;
		case "java.awt.Polygon":
			Polygon p = new Polygon();
			String[] splitted = strClusteringKey.replace("(", "").replace(")", "").split(",");
			int i = 0;
			while (i < splitted.length) {
				p.addPoint(Integer.parseInt(splitted[i]), Integer.parseInt(splitted[i + 1]));
				i += 2;
			}
			keyObject = p;
			break;
		}

		// get pages
		if (indices.contains(key)) {
			String path = "data/" + strTableName + key + ".class";
			switch (getColType(strTableName, key)) {
			case "java.lang.Integer":
				toUpdate.addAll(((BxTree<Integer, Integer>) loadObject(path)).search((Integer) keyObject));
				break;
			case "java.lang.String":
				toUpdate.addAll(((BxTree<String, Integer>) loadObject(path)).search((String) keyObject));
				break;
			case "java.lang.Double":
				toUpdate.addAll(((BxTree<Double, Integer>) loadObject(path)).search((Double) keyObject));
				break;
			case "java.lang.Boolean":
				toUpdate.addAll(((BxTree<Boolean, Integer>) loadObject(path)).search((Boolean) keyObject));
				break;
			case "java.util.Date":
				toUpdate.addAll(((BxTree<Date, Integer>) loadObject(path)).search((Date) keyObject));
			case "java.awt.Polygon":
			java.awt.Rectangle b = ((Polygon)keyObject).getBounds();
			Rectangle r = new Rectangle((float)b.getMinX(),(float)b.getMinY(),(float)b.getMaxX(),(float)b.getMaxY());
			((RTree)loadObject(path)).contains(r, new TIntProcedure() {
				@Override
				public boolean execute(int value) {
					toUpdate.add(value);
					return true;
				}
			});
			}
		}
				
		// update rows
		for (Integer page : toUpdate) {
			String file = "data/" + strTableName + page + ".class";
			Vector<Hashtable<String, Object>> rows = (Vector<Hashtable<String, Object>>) loadObject(file);
			for (Hashtable<String, Object> h : rows) {
				if (h.get(key).equals(keyObject)) {
					oldRows.put((Hashtable<String, Object>) h.clone(), page);
					for (String col : htblColNameValue.keySet()) {
						h.put(col, htblColNameValue.get(col));
					}
				}
			}
			saveObject(rows, file);
		}

		// update index
		for (String s : indices) {
			if (htblColNameValue.keySet().contains(s)) {
				String file = "data/" + strTableName + s + ".class";
				for (Hashtable<String, Object> h : oldRows.keySet()) {
					HashSet<Integer> hs = new HashSet<Integer>();
					switch (getColType(strTableName, s)) {
					case "java.lang.Integer":
						BxTree<Integer, Integer> bti = (BxTree<Integer, Integer>) loadObject(file);
						hs.addAll(bti.search((Integer) h.get(s)));
						hs.remove(oldRows.get(h));
						bti.delete((Integer) h.get(s));
						for (Integer v : hs) {
							bti.insert((Integer) h.get(s), v);
						}
						bti.insert((Integer) htblColNameValue.get(s), oldRows.get(h));
						saveObject(bti, file);
						break;
					case "java.lang.String":
						BxTree<String, Integer> bts = (BxTree<String, Integer>) loadObject(file);
						hs.addAll(bts.search((String) h.get(s)));
						hs.remove(oldRows.get(h));
						bts.delete((String) h.get(s));
						for (Integer v : hs) {
							bts.insert((String) h.get(s), v);
						}
						bts.insert((String) htblColNameValue.get(s), oldRows.get(h));
						saveObject(bts, file);
						break;
					case "java.lang.Double":
						BxTree<Double, Integer> btd = (BxTree<Double, Integer>) loadObject(file);
						hs.addAll(btd.search((Double) h.get(s)));
						hs.remove(oldRows.get(h));
						btd.delete((Double) h.get(s));
						for (Integer v : hs) {
							btd.insert((Double) h.get(s), v);
						}
						btd.insert((Double) htblColNameValue.get(s), oldRows.get(h));
						saveObject(btd, file);
						break;
					case "java.lang.Boolean":
						BxTree<Boolean, Integer> btb = (BxTree<Boolean, Integer>) loadObject(file);
						hs.addAll(btb.search((Boolean) h.get(s)));
						hs.remove(oldRows.get(h));
						btb.delete((Boolean) h.get(s));
						for (Integer v : hs) {
							btb.insert((Boolean) h.get(s), v);
						}
						btb.insert((Boolean) htblColNameValue.get(s), oldRows.get(h));
						saveObject(btb, file);
						break;
					case "java.util.Date":
						BxTree<Date, Integer> btda = (BxTree<Date, Integer>) loadObject(file);
						hs.addAll(btda.search((Date) h.get(s)));
						hs.remove(oldRows.get(h));
						btda.delete((Date) h.get(s));
						for (Integer v : hs) {
							btda.insert((Date) h.get(s), v);
						}
						btda.insert((Date) htblColNameValue.get(s), oldRows.get(h));
						saveObject(btda, file);
						break;
					case "java.awt.Polygon":
						RTree rtree = (RTree) loadObject(file);
						java.awt.Rectangle b = ((Polygon) h.get(s)).getBounds();
						Rectangle r = new Rectangle((float) b.getMinX(), (float) b.getMinY(), (float) b.getMaxX(),
								(float) b.getMaxY());
						rtree.delete(r, oldRows.get(h));
						b = ((Polygon) htblColNameValue.get(s)).getBounds();
						r = new Rectangle((float) b.getMinX(), (float) b.getMinY(), (float) b.getMaxX(),
								(float) b.getMaxY());
						rtree.add(r, oldRows.get(h));
						saveObject(rtree, file);
					}
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		ArrayList<String> indices = getTableIndexed(strTableName);
		HashSet<Integer> toDelete = new HashSet<Integer>();
		Hashtable<Hashtable<String, Object>, Integer> oldRows = new Hashtable<Hashtable<String, Object>, Integer>();

		// get pages
		for (String s : indices) {
			if (htblColNameValue.keySet().contains(s)) {
				String path = "data/" + strTableName + s + ".class";
				switch (getColType(strTableName, s)) {
				case "java.lang.Integer":
					toDelete.addAll(
							((BxTree<Integer, Integer>) loadObject(path)).search((Integer) htblColNameValue.get(s))); break;
				case "java.lang.String":
					toDelete.addAll(
							((BxTree<String, Integer>) loadObject(path)).search((String) htblColNameValue.get(s))); break;
				case "java.lang.Double":
					toDelete.addAll(
							((BxTree<Double, Integer>) loadObject(path)).search((Double) htblColNameValue.get(s))); break;
				case "java.lang.Boolean":
					toDelete.addAll(
							((BxTree<Boolean, Integer>) loadObject(path)).search((Boolean) htblColNameValue.get(s))); break;
				case "java.util.Date":
					toDelete.addAll(((BxTree<Date, Integer>) loadObject(path)).search((Date) htblColNameValue.get(s))); break;
				case "java.awt.Polygon":
					java.awt.Rectangle b = ((Polygon)htblColNameValue.get(s)).getBounds();
					Rectangle r = new Rectangle((float)b.getMinX(),(float)b.getMinY(),(float)b.getMaxX(),(float)b.getMaxY());
					((RTree)loadObject(path)).contains(r, new TIntProcedure() {
						@Override
						public boolean execute(int value) {
							toDelete.add(value);
							return true;
						}
					});
					}
			}
		}
		
		// delete pages
		for (Integer page : toDelete) {
			String file = "data/" + strTableName + page + ".class";
			Vector<Hashtable<String, Object>> rows = (Vector<Hashtable<String, Object>>) loadObject(file);
			Vector<Hashtable<String, Object>> remove = new Vector<Hashtable<String, Object>>();
			for (Hashtable<String, Object> h : rows) {
				boolean delete = true;
				for (String col : htblColNameValue.keySet()) {
					delete = h.get(col).equals(htblColNameValue.get(col));
				}
				if (delete) {
					oldRows.put((Hashtable<String, Object>) h.clone(), page);
					remove.add(h);
				}
			}
			rows.removeAll(remove);
			saveObject(rows, file);
		}

		// update index
		for (String s : indices) {
			String file = "data/" + strTableName + s + ".class";
			for (Hashtable<String, Object> h : oldRows.keySet()) {
				HashSet<Integer> hs = new HashSet<Integer>();
				switch (getColType(strTableName, s)) {
				case "java.lang.Integer":
					BxTree<Integer, Integer> bti = (BxTree<Integer, Integer>) loadObject(file);
					hs.addAll(bti.search((Integer) h.get(s)));
					hs.remove(oldRows.get(h));
					bti.delete((Integer) h.get(s));
					for (Integer v : hs) {
						bti.insert((Integer) h.get(s), v);
					}
					saveObject(bti, file);
					break;
				case "java.lang.String":
					BxTree<String, Integer> bts = (BxTree<String, Integer>) loadObject(file);
					hs.addAll(bts.search((String) h.get(s)));
					hs.remove(oldRows.get(h));
					bts.delete((String) h.get(s));
					for (Integer v : hs) {
						bts.insert((String) h.get(s), v);
					}
					saveObject(bts, file);
					break;
				case "java.lang.Double":
					BxTree<Double, Integer> btd = (BxTree<Double, Integer>) loadObject(file);
					hs.addAll(btd.search((Double) h.get(s)));
					hs.remove(oldRows.get(h));
					btd.delete((Double) h.get(s));
					for (Integer v : hs) {
						btd.insert((Double) h.get(s), v);
					}
					saveObject(btd, file);
					break;
				case "java.lang.Boolean":
					BxTree<Boolean, Integer> btb = (BxTree<Boolean, Integer>) loadObject(file);
					hs.addAll(btb.search((Boolean) h.get(s)));
					hs.remove(oldRows.get(h));
					btb.delete((Boolean) h.get(s));
					for (Integer v : hs) {
						btb.insert((Boolean) h.get(s), v);
					}
					saveObject(btb, file);
					break;
				case "java.util.Date":
					BxTree<Date, Integer> btda = (BxTree<Date, Integer>) loadObject(file);
					hs.addAll(btda.search((Date) h.get(s)));
					hs.remove(oldRows.get(h));
					btda.delete((Date) h.get(s));
					for (Integer v : hs) {
						btda.insert((Date) h.get(s), v);
					}
					saveObject(btda, file);
					break;
				case "java.awt.Polygon":
					RTree rtree = (RTree) loadObject(file);
					java.awt.Rectangle b = ((Polygon) h.get(s)).getBounds();
					Rectangle r = new Rectangle((float) b.getMinX(), (float) b.getMinY(), (float) b.getMaxX(),
							(float) b.getMaxY());
					rtree.delete(r, oldRows.get(h));
					saveObject(rtree, file);
				}
			}
		}
	}

	
	@SuppressWarnings("unchecked")
	public Vector<Hashtable<String, Object>> linearSearch(SQLTerm sql) {
		Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();
		int page = 0;
		String file = "data/" + sql._strTableName + page + ".class";
		while ((new File(file)).exists()) {
			Vector<Hashtable<String, Object>> rows = (Vector<Hashtable<String, Object>>) loadObject(file);
			for (Hashtable<String, Object> h : rows) {
				int compare = 0;
				switch (sql._objValue.getClass().getSimpleName()) {
				case "Integer":
					compare = Integer.compare((int) h.get(sql._strColumnName), (int) sql._objValue);
					break;
				case "Double":
					compare = Double.compare((double) h.get(sql._strColumnName), (double) sql._objValue);
					break;
				case "String":
					compare = ((String) h.get(sql._strColumnName)).compareTo((String) sql._objValue);
					break;
				case "Boolean":
					compare = Boolean.compare((boolean) h.get(sql._strColumnName), (boolean) sql._objValue);
					break;
				case "Date":
					compare = ((Date) h.get(sql._strColumnName)).compareTo((Date) sql._objValue);
					break;
				case "Polygon":
					Dimension d1 = ((java.awt.Polygon) h.get(sql._strColumnName)).getBounds().getSize();
					Dimension d2 = ((java.awt.Polygon) sql._objValue).getBounds().getSize();
					int a1 = d1.width * d1.height;
					int a2 = d2.width * d2.height;
					compare = Integer.compare(a1,a2);
					break;
				}
				switch (sql._strOperator) {
				case "=":
					if (compare == 0)
						result.add(h);
					break;
				case "!=":
					if (compare != 0)
						result.add(h);
					break;
				case "<=":
					if (compare == 0 || compare > 0)
						result.add(h);
					break;
				case ">=":
					if (compare == 0 || compare < 0)
						result.add(h);
					break;
				case "<":
					if (compare > 0)
						result.add(h);
					break;
				case ">":
					if (compare < 0)
						result.add(h);
					break;
				}
			}
			page++;
			file = "data/" + sql._strTableName + page + ".class";
		}
		return result;
	}

	
	@SuppressWarnings("unchecked")
	public Iterator<Hashtable<String, Object>> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
			throws DBAppException {

		Vector<Vector<Hashtable<String, Object>>> iterator = new Vector<Vector<Hashtable<String, Object>>>();

		for (int i = 0; i < arrSQLTerms.length; i++) {

			iterator.add(new Vector<Hashtable<String, Object>>());
			String key = getTableKey(arrSQLTerms[i]._strTableName);

			if (getTableIndexed(arrSQLTerms[i]._strTableName).contains(arrSQLTerms[i]._strColumnName)
					&& arrSQLTerms[i]._strOperator.contains("=")) {

				HashSet<Integer> select = new HashSet<Integer>();

				// get pages
				String path = "data/" + arrSQLTerms[i]._strTableName + arrSQLTerms[i]._strColumnName + ".class";
				switch (getColType(arrSQLTerms[i]._strTableName, arrSQLTerms[i]._strColumnName)) {
				case "java.lang.Integer":
					select.addAll(
							((BxTree<Integer, Integer>) loadObject(path)).search((Integer) arrSQLTerms[i]._objValue));
					break;
				case "java.lang.String":
					select.addAll(
							((BxTree<String, Integer>) loadObject(path)).search((String) arrSQLTerms[i]._objValue));
					break;
				case "java.lang.Double":
					select.addAll(
							((BxTree<Double, Integer>) loadObject(path)).search((Double) arrSQLTerms[i]._objValue));
					break;
				case "java.lang.Boolean":
					select.addAll(
							((BxTree<Boolean, Integer>) loadObject(path)).search((Boolean) arrSQLTerms[i]._objValue));
					break;
				case "java.util.Date":
					select.addAll(((BxTree<Date, Integer>) loadObject(path)).search((Date) arrSQLTerms[i]._objValue));
				case "java.awt.Polygon":
					java.awt.Rectangle b = ((Polygon)arrSQLTerms[i]._objValue).getBounds();
					Rectangle r = new Rectangle((float)b.getMinX(),(float)b.getMinY(),(float)b.getMaxX(),(float)b.getMaxY());
					((RTree)loadObject(path)).contains(r, new TIntProcedure() {
						@Override
						public boolean execute(int value) {
							select.add(value);
							return true;
						}
					});
				}

				// get query
				for (Integer page : select) {
					String file = "data/" + arrSQLTerms[i]._strTableName + page + ".class";
					Vector<Hashtable<String, Object>> rows = (Vector<Hashtable<String, Object>>) loadObject(file);
					for (Hashtable<String, Object> h : rows) {
						if (h.get(arrSQLTerms[i]._strColumnName).equals(arrSQLTerms[i]._objValue))
							iterator.get(i).add(h);
					}
				}
			} else if (arrSQLTerms[i]._strOperator.equals("=") && arrSQLTerms[i]._strColumnName.equals(key)) {
				int page = 0;
				String file = "data/" + arrSQLTerms[i]._strTableName + page + ".class";
				while ((new File(file)).exists()) {
					Vector<Hashtable<String, Object>> rows = (Vector<Hashtable<String, Object>>)loadObject(file);
					Object[] values = new Object[rows.size()];
					int counter = 0;
					for (Hashtable<String, Object> h : rows) {
						values[counter] = h.get(key);
					}
					int index = Arrays.binarySearch(values, arrSQLTerms[i]._objValue);
					iterator.get(i).add(rows.get(index));
					page ++;
					file = "data/" + arrSQLTerms[i]._strTableName + page + ".class";
				}
			}

			if (!(arrSQLTerms[i]._strOperator.equals("=") && (getTableIndexed(arrSQLTerms[i]._strTableName).contains(arrSQLTerms[i]._strColumnName)) || arrSQLTerms[i]._strColumnName.equals(key))) {
				iterator.get(i).addAll(linearSearch(arrSQLTerms[i]));
			}
		}
		
		for (int i = 1; i <= strarrOperators.length; i++) {
			iterator.add((i * 2), new Vector<Hashtable<String, Object>>());
			switch(strarrOperators[i - 1]) {
			case "XOR" :
				for (Hashtable<String, Object> hash : iterator.get((i * 2) - 2)) {
					if (!iterator.get((i * 2) - 1).contains(hash)) {
						iterator.get((i * 2)).add(hash);
					} else {
						iterator.get((i * 2) - 1).remove(hash);
					}
				}
				iterator.get((i * 2)).addAll(iterator.get((i * 2) - 1)); break;
			case "OR" :
				iterator.get((i * 2)).addAll(iterator.get((i * 2) - 2));
				for (Hashtable<String, Object> hash : iterator.get((i * 2) - 1)) {
					if (!iterator.get((i * 2) - 2).contains(hash)) {
						iterator.get((i * 2)).add(hash);
					}
				} break;
			case "AND" :
				for (Hashtable<String, Object> hash : iterator.get((i * 2) - 2)) {
					if (iterator.get((i * 2) - 1).contains(hash)) {
						iterator.get((i * 2)).add(hash);
					}
				} break;
			}
		}
		return iterator.lastElement().iterator();
	}

	public static void main(String[] args) {
		try {
			(new File("data/Student0.class")).delete();
			String strTableName = "Student";
			DBApp dbApp = new DBApp();
			dbApp.init();
			Hashtable htblColNameType = new Hashtable();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.Double");
			dbApp.createTable(strTableName, "id", htblColNameType);
			dbApp.createBTreeIndex( strTableName, "gpa" );
			Hashtable htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(2343432));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.7));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(453455));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(5674567));
			htblColNameValue.put("name", new String("Dalia Noor"));
			htblColNameValue.put("gpa", new Double(1.25));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(23498));
			htblColNameValue.put("name", new String("John Noor"));
			htblColNameValue.put("gpa", new Double(1.5));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(78452));
			htblColNameValue.put("name", new String("Zaky Noor"));
			htblColNameValue.put("gpa", new Double(0.88));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("gpa", new Double(5.0));
			dbApp.updateTable(strTableName, "78452", htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("gpa", new Double(0.88));
			dbApp.deleteFromTable(strTableName, htblColNameValue);
			SQLTerm[] arrSQLTerms;
			arrSQLTerms = new SQLTerm[2];
			arrSQLTerms[0] = new SQLTerm();
			arrSQLTerms[1] = new SQLTerm();
			arrSQLTerms[0]._strTableName = "Student";
			arrSQLTerms[0]._strColumnName = "name";
			arrSQLTerms[0]._strOperator = "=";
			arrSQLTerms[0]._objValue = "John Noor";
			arrSQLTerms[1]._strTableName = "Student";
			arrSQLTerms[1]._strColumnName = "gpa";
			arrSQLTerms[1]._strOperator = "<";
			arrSQLTerms[1]._objValue = new Double(0);
			String[] strarrOperators = new String[1];
			strarrOperators[0] = "XOR";
			// select * from Student where name = “John Noor” or gpa = 1.5;
			//Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
//			while (resultSet.hasNext()) {
//				System.out.println(resultSet.next());
//			}
			System.out.println(((Vector<Hashtable<String, Object>>)dbApp.loadObject("data/Student0.class")));
			System.out.println(((BxTree<Double, Integer>)dbApp.loadObject("data/Studentgpa.class")));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
