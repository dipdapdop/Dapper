using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Collections.Concurrent;
using System.Reflection.Emit;
using Dapper;
using static Dapper.Contrib.Extensions.SqlMapperExtensions;

#if COREFX
using DataException = System.InvalidOperationException;
#else
using System.Threading;
using Oracle.ManagedDataAccess.Client;
#endif

namespace Dapper.Contrib.Extensions
{
    public static partial class SqlMapperExtensions
    {
        // ReSharper disable once MemberCanBePrivate.Global
        public interface IProxy //must be kept public
        {
            bool IsDirty { get; set; }
        }

        public interface ITableNameMapper
        {
            string GetTableName(Type type);
        }

        public delegate string GetDatabaseTypeDelegate(IDbConnection connection);
        public delegate string TableNameMapperDelegate(Type type);
        public delegate string ColumNameMapperDelegate(PropertyInfo propertyInfo);

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> KeyProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> ExplicitKeyProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> TypeProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> ComputedProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> ColumnProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<Tuple<RuntimeTypeHandle, RuntimeTypeHandle>, string> GetQueries = new ConcurrentDictionary<Tuple<RuntimeTypeHandle, RuntimeTypeHandle>, string>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> TypeTableName = new ConcurrentDictionary<RuntimeTypeHandle, string>();

        private static readonly ConcurrentDictionary<PropertyInfo, string> PropertyInfoToColumnName = new ConcurrentDictionary<PropertyInfo, string>();


        private static readonly ISqlAdapter DefaultAdapter = new SqlServerAdapter();
        private static readonly Dictionary<string, ISqlAdapter> AdapterDictionary
            = new Dictionary<string, ISqlAdapter>
            {
                {"sqlconnection", new SqlServerAdapter()},
                {"sqlceconnection", new SqlCeServerAdapter()},
                {"npgsqlconnection", new PostgresAdapter()},
                {"sqliteconnection", new SQLiteAdapter()},
                {"mysqlconnection", new MySqlAdapter()}
#if! COREFX
                ,{"oracleconnection", new OracleAdapter()}
#endif
            };


        public static ColumNameMapperDelegate ColumnNameMapper;
        private static string PropertyInfoToColumnNameCache(PropertyInfo propertyInfo)
        {
            string name = null;

            //var ci = propertyInfo.GetCustomAttributes(false)
            //    .OfType<ColumnAttribute>();

            if (PropertyInfoToColumnName.TryGetValue(propertyInfo, out name))
            {
                return name;
            }
            if (ColumnNameMapper == null)
            {
                name = propertyInfo.Name;
            }
            // SIM: use the column attribute as defined on the property 
            // as an override to the 'fallback' general ColumnNameMapper strategy.
            // (See #32 for why we need it.)



            //if (hasColumnAttribute)
            //{
            //    name = 
            //}
            else
            {
                name = ColumnNameMapper(propertyInfo);
            }
            PropertyInfoToColumnName[propertyInfo] = name;
            return name;
        }


        private static List<PropertyInfo> ComputedPropertiesCache(Type type)
        {
            IEnumerable<PropertyInfo> pi;
            if (ComputedProperties.TryGetValue(type.TypeHandle, out pi))
            {
                return pi.ToList();
            }

            var computedProperties = TypePropertiesCache(type).Where(p => p.GetCustomAttributes(true).Any(a => a is ComputedAttribute)).ToList();

            ComputedProperties[type.TypeHandle] = computedProperties;
            return computedProperties;
        }

        private static List<PropertyInfo> ExplicitKeyPropertiesCache(Type type)
        {
            IEnumerable<PropertyInfo> pi;
            if (ExplicitKeyProperties.TryGetValue(type.TypeHandle, out pi))
            {
                return pi.ToList();
            }

            var explicitKeyProperties = TypePropertiesCache(type).Where(p => p.GetCustomAttributes(true).Any(a => a is ExplicitKeyAttribute)).ToList();

            ExplicitKeyProperties[type.TypeHandle] = explicitKeyProperties;
            return explicitKeyProperties;
        }

        private static List<PropertyInfo> KeyPropertiesCache(Type type)
        {

            IEnumerable<PropertyInfo> pi;
            if (KeyProperties.TryGetValue(type.TypeHandle, out pi))
            {
                return pi.ToList();
            }

            var allProperties = TypePropertiesCache(type);
            var keyProperties = allProperties.Where(p =>
            {
                return p.GetCustomAttributes(true).Any(a => a is KeyAttribute);
            }).ToList();

            if (keyProperties.Count == 0)
            {
                var idProp = allProperties.FirstOrDefault(p => p.Name.ToLower() == "id");
                if (idProp != null && !idProp.GetCustomAttributes(true).Any(a => a is ExplicitKeyAttribute))
                {
                    keyProperties.Add(idProp);
                }
            }

            KeyProperties[type.TypeHandle] = keyProperties;
            return keyProperties;
        }

        private static List<PropertyInfo> TypePropertiesCache(Type type)
        {
            IEnumerable<PropertyInfo> pis;
            if (TypeProperties.TryGetValue(type.TypeHandle, out pis))
            {
                return pis.ToList();
            }

            var properties = type.GetProperties().Where(IsWriteable).ToArray();
            TypeProperties[type.TypeHandle] = properties;
            return properties.ToList();
        }

        /// <summary>
        /// Added SIM:
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private static List<PropertyInfo> ColumnPropertiesCache(Type type)
        {
            IEnumerable<PropertyInfo> pi;
            if (ColumnProperties.TryGetValue(type.TypeHandle, out pi))
            {
                return pi.ToList();
            }

            var columnPropertiesCache = TypePropertiesCache(type).Where(p => p.GetCustomAttributes(true).Any(a => a is ColumnAttribute)).ToList();

            ColumnProperties[type.TypeHandle] = columnPropertiesCache;
            return columnPropertiesCache;
        }

        private static bool IsWriteable(PropertyInfo pi)
        {
            var attributes = pi.GetCustomAttributes(typeof(WriteAttribute), false).AsList();
            if (attributes.Count != 1) return true;

            var writeAttribute = (WriteAttribute)attributes[0];
            return writeAttribute.Write;
        }

        private static List<PropertyInfo> GetAllKeys(Type type)
        {
            var keyProperties = KeyPropertiesCache(type).ToList();  //added ToList() due to issue #418, must work on a list copy
            var explicitKeyProperties = ExplicitKeyPropertiesCache(type);

            keyProperties.AddRange(explicitKeyProperties);

            if (!keyProperties.Any())
                throw new ArgumentException("Entity must have at least one [Key] or [ExplicitKey] property");

            return keyProperties;
        }

        private static string GenerateGetQuery(IDbConnection connection, Type type, dynamic identity, List<PropertyInfo> keyProperties)
        {
            string sql;
            Type identityType = identity?.GetType();
            var cacheKey = new Tuple<RuntimeTypeHandle, RuntimeTypeHandle>(type.TypeHandle, identityType.TypeHandle);

            // Generate query
            if (!GetQueries.TryGetValue(cacheKey, out sql))
            //if (!GetQueries.TryGetValue(identityType.TypeHandle, out sql))
            {
                var name = GetTableName(type);

                if (keyProperties.Count == 1)
                {
                    var key = keyProperties.First();
                    sql = $"select * from {name} where {PropertyInfoToColumnNameCache(key)} = :{key.Name}";
                }
                else
                {
                    var sb = new StringBuilder();
                    sb.Append($"select * from {name} where ");

                    var adapter = GetFormatter(connection);

                    for (var i = 0; i < keyProperties.Count; i++)
                    {

                        // SIM: HACK: AppendColumnNameEqualsValue takes a string not a PropertyInfo type.
                        //adapter.AppendColumnNameEqualsValue(sb, property.Name);  //fix for issue #336 (dapper)
                        {

                            var isParamSameType = identityType == type;
                            var keyTypeProperties = isParamSameType ? null : identityType.GetProperties();

                            var property = keyProperties.ElementAt(i);
                            var parameterName = property.Name;

                            // if key object passed is the same type as expected result, use property
                            var paramProperty = isParamSameType ? property :
                                keyTypeProperties.FirstOrDefault(
                                    p => p.Name.Equals(property.Name, StringComparison.CurrentCultureIgnoreCase)
                                );

                            dynamic paramPropertyValue = paramProperty?.GetValue(identity, null);
                            bool isArray = paramPropertyValue?.GetType().IsArray ?? default(bool);

                            sb.Append("(");
                            adapter.AppendColumnName(sb, PropertyInfoToColumnNameCache(property));

                            // #20 LGH/lgh-dapper-api
                            sb.Append(isArray ? " in " : " = "); // isParamSameType may be enough

                            if (isArray)
                            {
                                sb.Append("(");
                                var j = 1;

                                // isArray should be the guard clause here.
                                // ReSharper disable once PossibleNullReferenceException
                                // ReSharper disable once UnusedVariable
                                foreach (dynamic ppv in paramPropertyValue)
                                {
                                    adapter.AppendParameter(sb, parameterName + j); //implicit cast
                                    if (j < paramPropertyValue.Length)
                                        sb.Append(", ");
                                    j++;
                                }
                                // SIM: we don't need the null test in here.
                                sb.Append(")");
                                sb.Append(")");
                            }
                            else
                            {
                                adapter.AppendParameter(sb, parameterName);
                                sb.Append(" or ");
                                adapter.AppendParameter(sb, parameterName);
                                sb.Append(" is null");
                                sb.Append(")");
                            }
                        }

                        if (i < keyProperties.Count - 1)
                            sb.AppendFormat(" and ");
                    }

                    sql = sb.ToString();
                }

                GetQueries[cacheKey] = sql;
                //GetQueries[identityType.TypeHandle] = sql;
            }

            return sql;
        }

        private static DynamicParameters GenerateGetParams(Type type, dynamic identity, List<PropertyInfo> keyProperties)
        {

            var dynParms = new DynamicParameters();
            Type identityType = identity?.GetType();

            if (keyProperties.Count == 1 && (identityType == null || identityType.IsValueType()))
            {
                dynParms.Add($":{keyProperties.First().Name}", identity);
            }
            else
            {
                if (identityType == null || identityType.IsValueType())
                    throw new ArgumentException($"The key object passed cannot be null or value type for composite key in {type.Name}");

                var isParamSameType = identityType == type;
                var keyTypeProperties = isParamSameType ? null : identityType.GetProperties();

                for (var i = 0; i < keyProperties.Count; i++)
                {
                    var property = keyProperties.ElementAt(i);

                    // if key object passed is the same type as expected result, use property
                    var paramProperty = isParamSameType ? property :
                        keyTypeProperties.FirstOrDefault(
                            p => p.Name.Equals(property.Name, StringComparison.CurrentCultureIgnoreCase)
                        );

                    dynamic paramPropertyValue = paramProperty?.GetValue(identity, null);
                    bool isArray = paramPropertyValue?.GetType().IsArray ?? default(bool);

                    // SIM: not sure that this is necessary or desirable - we test against null in generated sql
                    // as is the house style.
                    //if (paramProperty == null)
                    //    throw new ArgumentException($"The key object passed does not contain {property.Name} property.");

                    // SIM: now test for null here using conditional access, will pass in null to dynParms list
                    // (if null reference) which is the desired behaviour.
                    //dynParms.Add($":{property.Name}", paramProperty.GetValue(identity, null));

                    if (isArray)
                    {
                        var j = 1;
                        // isArray should be the guard clause here.
                        // ReSharper disable once PossibleNullReferenceException
                        foreach (dynamic ppv in paramPropertyValue)
                        {
                            dynParms.Add($":{property.Name}{j}", ppv);
                            j++;
                        }
                    }
                    else
                    {
                        dynParms.Add($":{property.Name}", paramProperty?.GetValue(identity, null));
                    }
                }
            }

            return dynParms;
        }

        /// <summary>
        /// Returns a single entity by a key from table "Ts".  
        /// Keys must be marked with [Key] or [ExplicitKey] attributes.
        /// Entities created from interfaces are tracked/intercepted for changes and used by the Update() extension
        /// for optimal performance. 
        /// </summary>
        /// <typeparam name="T">Interface or type to create and populate</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        ///<param name="identity">Single or composite Key object that represents the entity to get</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>Entity of T</returns>
        public static T Get<T>(this IDbConnection connection, dynamic identity, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var type = typeof(T);

            List<PropertyInfo> keyProperties = GetAllKeys(type);

            var sql = GenerateGetQuery(connection, type, (object)identity, keyProperties);
            DynamicParameters dynParms = GenerateGetParams(type, identity, keyProperties);

            T obj;

            if (type.IsInterface())
            {
                var res = connection.Query(sql, dynParms).FirstOrDefault() as IDictionary<string, object>;

                if (res == null)
                    return null;

                obj = ProxyGenerator.GetInterfaceProxy<T>();

                foreach (var property in TypePropertiesCache(type))
                {
                    var val = res[PropertyInfoToColumnNameCache(property)];
                    property.SetValue(obj, Convert.ChangeType(val, property.PropertyType), null);
                }

                ((IProxy)obj).IsDirty = false;   //reset change tracking and return
            }
            else
            {
                obj = connection.Query<T>(sql, dynParms, transaction, commandTimeout: commandTimeout).FirstOrDefault();
            }
            return obj;
        }

        /// <summary>
        /// Returns a list of entites from table "Ts".  
        /// Id of T must be marked with [Key] attribute.
        /// Entities created from interfaces are tracked/intercepted for changes and used by the Update() extension
        /// for optimal performance. 
        /// </summary>
        /// <typeparam name="T">Interface or type to create and populate</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>Entity of T</returns>
        public static IEnumerable<T> GetAll<T>(this IDbConnection connection, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var type = typeof(T);
            var cacheType = typeof(List<T>);

            // SIM: type.TypeHandle, placeholder for identity
            var cacheKey = new Tuple<RuntimeTypeHandle, RuntimeTypeHandle>(cacheType.TypeHandle, type.TypeHandle);

            string sql;
            if (!GetQueries.TryGetValue(cacheKey, out sql))
            {
                //GetSingleKey<T>(nameof(GetAll));
                var name = GetTableName(type);

                sql = "select * from " + name;
                GetQueries[cacheKey] = sql;
            }

            if (!type.IsInterface()) return connection.Query<T>(sql, null, transaction, commandTimeout: commandTimeout);

            var result = connection.Query(sql);
            var list = new List<T>();
            foreach (IDictionary<string, object> res in result)
            {
                var obj = ProxyGenerator.GetInterfaceProxy<T>();
                foreach (var property in TypePropertiesCache(type))
                {
                    var val = res[PropertyInfoToColumnNameCache(property)];
                    property.SetValue(obj, Convert.ChangeType(val, property.PropertyType), null);
                }
                ((IProxy)obj).IsDirty = false;   //reset change tracking and return
                list.Add(obj);
            }
            return list;

        }

        /*
                public static IEnumerable<T> Query<T>(this IDbConnection connection, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
                {
                    var type = typeof(T);
                    var cacheType = typeof(List<T>);

                    string sql;
                    if (!GetQueries.TryGetValue(cacheType.TypeHandle, out sql))
                    {
                        var key = GetSingleKey<T>(nameof(Query));
                        var name = GetTableName(type);

                        //sql = "select * from " + name;
                        sql = $"select * from {name} where {PropertyInfoToColumnNameCache(key)} = :{key.Name}";
                        GetQueries[cacheType.TypeHandle] = sql;
                    }

                    if (!type.IsInterface) return connection.Query<T>(sql, null, transaction, commandTimeout: commandTimeout);

                    var result = connection.Query(sql);
                    var list = new List<T>();
                    foreach (IDictionary<string, object> res in result)
                    {
                        var obj = ProxyGenerator.GetInterfaceProxy<T>();
                        foreach (var property in TypePropertiesCache(type))
                        {
                            var val = res[PropertyInfoToColumnNameCache(property)];
                            property.SetValue(obj, Convert.ChangeType(val, property.PropertyType), null);
                        }
                        ((IProxy)obj).IsDirty = false;   //reset change tracking and return
                        list.Add(obj);
                    }
                    return list;

                }
        */

        /// <summary>
        /// Returns a list of entites from table "Ts".  
        /// Id of T must be marked with [Key] attribute.
        /// Entities created from interfaces are tracked/intercepted for changes and used by the Update() extension
        /// for optimal performance. 
        /// </summary>
        /// <typeparam name="T">Interface or type to create and populate</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="identity"></param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>Entity of T</returns>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection, dynamic /*id*/ identity, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var type = typeof(T);
            List<PropertyInfo> keyProperties = GetAllKeys(type);
            // SIM: NB: do not change this to 'var' or else connection.Query call will complain
            // about dynamic dispatch.
            string sql = GenerateGetQuery(connection, type, identity, keyProperties);
            DynamicParameters dynParms = GenerateGetParams(type, identity, keyProperties);

            var list = new List<T>();
            if (type.IsInterface())
            {
                // SIM: this call: 
                var res = connection.Query(sql, dynParms).FirstOrDefault() as IDictionary<string, object>;

                if (res == null)
                    return null;

                var obj = ProxyGenerator.GetInterfaceProxy<T>();

                foreach (var property in TypePropertiesCache(type))
                {
                    var val = res[PropertyInfoToColumnNameCache(property)];
                    property.SetValue(obj, Convert.ChangeType(val, property.PropertyType), null);
                }

                ((IProxy)obj).IsDirty = false;   //reset change tracking and return
                list.Add(obj);
            }
            else
            {
                list = connection.Query<T>(sql, dynParms, transaction, commandTimeout: commandTimeout).ToList();
            }
            return list;

        }

        /// <summary>
        /// Specify a custom table name mapper based on the POCO type name
        /// </summary>
        public static TableNameMapperDelegate TableNameMapper;

        private static string GetTableName(Type type)
        {
            string name;
            if (TypeTableName.TryGetValue(type.TypeHandle, out name)) return name;

            if (TableNameMapper != null)
            {
                name = TableNameMapper(type);
            }
            else
            {
                //NOTE: This 'as dynamic' trick should be able to handle both our own Table-attribute as well as the one in EntityFramework 
                var tableAttr = type
#if COREFX
                    .GetTypeInfo()
#endif
                    .GetCustomAttributes(false).SingleOrDefault(attr => attr.GetType().Name == "TableAttribute") as dynamic;
                if (tableAttr != null)
                    name = tableAttr.Name;
                else
                {
                    name = type.Name + "s";
                    if (type.IsInterface() && name.StartsWith("I"))
                        name = name.Substring(1);
                }
            }

            TypeTableName[type.TypeHandle] = name;
            return name;
        }


        /// <summary>
        /// Inserts an entity into table "Ts" and returns identity id or number if inserted rows if inserting a list.
        /// </summary>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToInsert">Entity to insert, can be list of entities</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>Identity of inserted entity, or number of inserted rows if inserting a list</returns>
        public static long Insert<T>(this IDbConnection connection, T entityToInsert, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var isList = false;

            var type = typeof(T);

            if (type.IsArray)
            {
                isList = true;
                type = type.GetElementType();
            }
            else if (type.IsGenericType())
            {
                isList = true;
                type = type.GetGenericArguments()[0];
            }

            var name = GetTableName(type);
            var sbColumnList = new StringBuilder(null);
            var allProperties = TypePropertiesCache(type);
            var keyProperties = KeyPropertiesCache(type);
            var computedProperties = ComputedPropertiesCache(type);
            var columnProperties = ColumnPropertiesCache(type); //TODO: 
            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            var adapter = GetFormatter(connection);

            for (var i = 0; i < allPropertiesExceptKeyAndComputed.Count; i++)
            {
                var property = allPropertiesExceptKeyAndComputed.ElementAt(i);
                adapter.AppendColumnName(sbColumnList, PropertyInfoToColumnNameCache(property));  //fix for issue #336
                if (i < allPropertiesExceptKeyAndComputed.Count - 1)
                    sbColumnList.Append(", ");
            }

            var sbParameterList = new StringBuilder(null);
            for (var i = 0; i < allPropertiesExceptKeyAndComputed.Count; i++)
            {
                var property = allPropertiesExceptKeyAndComputed.ElementAt(i);
                //sbParameterList.Append($"@{property.Name}");
                sbParameterList.Append($":{property.Name}");
                if (i < allPropertiesExceptKeyAndComputed.Count - 1)
                    sbParameterList.Append(", ");
            }

            int returnVal;
            var wasClosed = connection.State == ConnectionState.Closed;
            if (wasClosed) connection.Open();

            if (!isList)    //single entity
            {
                returnVal = adapter.Insert(connection, transaction, commandTimeout, name, sbColumnList.ToString(),
                    sbParameterList.ToString(), keyProperties, ColumnNameMapper, entityToInsert);
            }
            else
            {
                //insert list of entities
                var cmd = $"insert into {name} ({sbColumnList}) values ({sbParameterList})";
                returnVal = connection.Execute(cmd, entityToInsert, transaction, commandTimeout);
            }
            if (wasClosed) connection.Close();
            return returnVal;
        }

        /// <summary>
        /// Updates entity in table "Ts", checks if the entity is modified if the entity is tracked by the Get() extension.
        /// </summary>
        /// <typeparam name="T">Type to be updated</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToUpdate">Entity to be updated</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>true if updated, false if not found or not modified (tracked entities)</returns>
        public static bool Update<T>(this IDbConnection connection, T entityToUpdate, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var proxy = entityToUpdate as IProxy;
            if (proxy != null)
            {
                if (!proxy.IsDirty) return false;
            }

            var type = typeof(T);

            if (type.IsArray)
            {
                type = type.GetElementType();
            }
            else if (type.IsGenericType())
            {
                type = type.GetGenericArguments()[0];
            }

            //var keyProperties = KeyPropertiesCache(type).ToList();  //added ToList() due to issue #418, must work on a list copy
            //var explicitKeyProperties = ExplicitKeyPropertiesCache(type);
            //if (!keyProperties.Any() && !explicitKeyProperties.Any())
            //    throw new ArgumentException("Entity must have at least one [Key] or [ExplicitKey] property");

            var keyProperties = GetAllKeys(type);

            var name = GetTableName(type);

            var sb = new StringBuilder();
            sb.Append($"update {name} set ");

            var allProperties = TypePropertiesCache(type);
            //keyProperties.AddRange(explicitKeyProperties);
            var computedProperties = ComputedPropertiesCache(type);
            var nonIdProps = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            var adapter = GetFormatter(connection);

            for (var i = 0; i < nonIdProps.Count; i++)
            {
                var property = nonIdProps.ElementAt(i);
                adapter.AppendColumnNameEqualsValue(sb, PropertyInfoToColumnNameCache(property));  //fix for issue #336
                if (i < nonIdProps.Count - 1)
                    sb.AppendFormat(", ");
            }
            sb.Append(" where ");
            for (var i = 0; i < keyProperties.Count; i++)
            {
                var property = keyProperties.ElementAt(i);
                adapter.AppendColumnNameEqualsValue(sb, PropertyInfoToColumnNameCache(property));  //fix for issue #336
                if (i < keyProperties.Count - 1)
                    sb.AppendFormat(" and ");
            }
            var updated = connection.Execute(sb.ToString(), entityToUpdate, commandTimeout: commandTimeout, transaction: transaction);
            return updated > 0;
        }

        /// <summary>
        /// Delete entity in table "Ts".
        /// </summary>
        /// <typeparam name="T">Type of entity</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToDelete">Entity to delete</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>true if deleted, false if not found</returns>
        public static bool Delete<T>(this IDbConnection connection, T entityToDelete, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            if (entityToDelete == null)
                throw new ArgumentException("Cannot Delete null Object", nameof(entityToDelete));

            var type = typeof(T);

            if (type.IsArray)
            {
                type = type.GetElementType();
            }
            else if (type.IsGenericType())
            {
                type = type.GetGenericArguments()[0];
            }

            var keyProperties = GetAllKeys(type);

            var name = GetTableName(type);

            var sb = new StringBuilder();
            sb.Append($"delete from {name} where ");

            var adapter = GetFormatter(connection);

            for (var i = 0; i < keyProperties.Count; i++)
            {
                var property = keyProperties.ElementAt(i);
                adapter.AppendColumnNameEqualsValue(sb, PropertyInfoToColumnNameCache(property));  //fix for issue #336
                if (i < keyProperties.Count - 1)
                    sb.AppendFormat(" and ");
            }
            var deleted = connection.Execute(sb.ToString(), entityToDelete, transaction, commandTimeout);
            return deleted > 0;
        }

        /// <summary>
        /// Delete all entities in the table related to the type T.
        /// </summary>
        /// <typeparam name="T">Type of entity</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>true if deleted, false if none found</returns>
        public static bool DeleteAll<T>(this IDbConnection connection, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var type = typeof(T);
            var name = GetTableName(type);
            var statement = $"delete from {name}";
            var deleted = connection.Execute(statement, null, transaction, commandTimeout);
            return deleted > 0;
        }

        /// <summary>
        /// Specifies a custom callback that detects the database type instead of relying on the default strategy (the name of the connection type object).
        /// Please note that this callback is global and will be used by all the calls that require a database specific adapter.
        /// </summary>
        public static GetDatabaseTypeDelegate GetDatabaseType;

        private static ISqlAdapter GetFormatter(IDbConnection connection)
        {
            var name = GetDatabaseType?.Invoke(connection).ToLower()
                       ?? connection.GetType().Name.ToLower();

            return !AdapterDictionary.ContainsKey(name)
                ? DefaultAdapter
                : AdapterDictionary[name];
        }

        static class ProxyGenerator
        {
            private static readonly Dictionary<Type, Type> TypeCache = new Dictionary<Type, Type>();

            private static AssemblyBuilder GetAsmBuilder(string name)
            {
#if COREFX
                return AssemblyBuilder.DefineDynamicAssembly(new AssemblyName { Name = name }, AssemblyBuilderAccess.Run);
#else
                return Thread.GetDomain().DefineDynamicAssembly(new AssemblyName { Name = name }, AssemblyBuilderAccess.Run);
#endif
            }

            public static T GetInterfaceProxy<T>()
            {
                Type typeOfT = typeof(T);

                Type k;
                if (TypeCache.TryGetValue(typeOfT, out k))
                {
                    return (T)Activator.CreateInstance(k);
                }
                var assemblyBuilder = GetAsmBuilder(typeOfT.Name);

                var moduleBuilder = assemblyBuilder.DefineDynamicModule("SqlMapperExtensions." + typeOfT.Name); //NOTE: to save, add "asdasd.dll" parameter

                var interfaceType = typeof(IProxy);
                var typeBuilder = moduleBuilder.DefineType(typeOfT.Name + "_" + Guid.NewGuid(),
                    TypeAttributes.Public | TypeAttributes.Class);
                typeBuilder.AddInterfaceImplementation(typeOfT);
                typeBuilder.AddInterfaceImplementation(interfaceType);

                //create our _isDirty field, which implements IProxy
                var setIsDirtyMethod = CreateIsDirtyProperty(typeBuilder);

                // Generate a field for each property, which implements the T
                foreach (var property in typeof(T).GetProperties())
                {
                    var isId = property.GetCustomAttributes(true).Any(a => a is KeyAttribute);
                    CreateProperty<T>(typeBuilder, property.Name, property.PropertyType, setIsDirtyMethod, isId);
                }

#if COREFX
                var generatedType = typeBuilder.CreateTypeInfo().AsType();
#else
                var generatedType = typeBuilder.CreateType();
#endif

                TypeCache.Add(typeOfT, generatedType);
                return (T)Activator.CreateInstance(generatedType);
            }


            private static MethodInfo CreateIsDirtyProperty(TypeBuilder typeBuilder)
            {
                var propType = typeof(bool);
                var field = typeBuilder.DefineField("_" + "IsDirty", propType, FieldAttributes.Private);
                var property = typeBuilder.DefineProperty("IsDirty",
                                               System.Reflection.PropertyAttributes.None,
                                               propType,
                                               new[] { propType });

                const MethodAttributes getSetAttr = MethodAttributes.Public | MethodAttributes.NewSlot | MethodAttributes.SpecialName |
                                                    MethodAttributes.Final | MethodAttributes.Virtual | MethodAttributes.HideBySig;

                // Define the "get" and "set" accessor methods
                var currGetPropMthdBldr = typeBuilder.DefineMethod("get_" + "IsDirty",
                                             getSetAttr,
                                             propType,
                                             Type.EmptyTypes);
                var currGetIl = currGetPropMthdBldr.GetILGenerator();
                currGetIl.Emit(OpCodes.Ldarg_0);
                currGetIl.Emit(OpCodes.Ldfld, field);
                currGetIl.Emit(OpCodes.Ret);
                var currSetPropMthdBldr = typeBuilder.DefineMethod("set_" + "IsDirty",
                                             getSetAttr,
                                             null,
                                             new[] { propType });
                var currSetIl = currSetPropMthdBldr.GetILGenerator();
                currSetIl.Emit(OpCodes.Ldarg_0);
                currSetIl.Emit(OpCodes.Ldarg_1);
                currSetIl.Emit(OpCodes.Stfld, field);
                currSetIl.Emit(OpCodes.Ret);

                property.SetGetMethod(currGetPropMthdBldr);
                property.SetSetMethod(currSetPropMthdBldr);
                var getMethod = typeof(IProxy).GetMethod("get_" + "IsDirty");
                var setMethod = typeof(IProxy).GetMethod("set_" + "IsDirty");
                typeBuilder.DefineMethodOverride(currGetPropMthdBldr, getMethod);
                typeBuilder.DefineMethodOverride(currSetPropMthdBldr, setMethod);

                return currSetPropMthdBldr;
            }

            private static void CreateProperty<T>(TypeBuilder typeBuilder, string propertyName, Type propType, MethodInfo setIsDirtyMethod, bool isIdentity)
            {
                //Define the field and the property 
                var field = typeBuilder.DefineField("_" + propertyName, propType, FieldAttributes.Private);
                var property = typeBuilder.DefineProperty(propertyName,
                                               System.Reflection.PropertyAttributes.None,
                                               propType,
                                               new[] { propType });

                const MethodAttributes getSetAttr = MethodAttributes.Public | MethodAttributes.Virtual |
                                                    MethodAttributes.HideBySig;

                // Define the "get" and "set" accessor methods
                var currGetPropMthdBldr = typeBuilder.DefineMethod("get_" + propertyName,
                                             getSetAttr,
                                             propType,
                                             Type.EmptyTypes);

                var currGetIl = currGetPropMthdBldr.GetILGenerator();
                currGetIl.Emit(OpCodes.Ldarg_0);
                currGetIl.Emit(OpCodes.Ldfld, field);
                currGetIl.Emit(OpCodes.Ret);

                var currSetPropMthdBldr = typeBuilder.DefineMethod("set_" + propertyName,
                                             getSetAttr,
                                             null,
                                             new[] { propType });

                //store value in private field and set the isdirty flag
                var currSetIl = currSetPropMthdBldr.GetILGenerator();
                currSetIl.Emit(OpCodes.Ldarg_0);
                currSetIl.Emit(OpCodes.Ldarg_1);
                currSetIl.Emit(OpCodes.Stfld, field);
                currSetIl.Emit(OpCodes.Ldarg_0);
                currSetIl.Emit(OpCodes.Ldc_I4_1);
                currSetIl.Emit(OpCodes.Call, setIsDirtyMethod);
                currSetIl.Emit(OpCodes.Ret);

                //TODO: Should copy all attributes defined by the interface?
                if (isIdentity)
                {
                    var keyAttribute = typeof(KeyAttribute);
                    var myConstructorInfo = keyAttribute.GetConstructor(new Type[] { });
                    var attributeBuilder = new CustomAttributeBuilder(myConstructorInfo, new object[] { });
                    property.SetCustomAttribute(attributeBuilder);
                }

                property.SetGetMethod(currGetPropMthdBldr);
                property.SetSetMethod(currSetPropMthdBldr);
                var getMethod = typeof(T).GetMethod("get_" + propertyName);
                var setMethod = typeof(T).GetMethod("set_" + propertyName);
                typeBuilder.DefineMethodOverride(currGetPropMthdBldr, getMethod);
                typeBuilder.DefineMethodOverride(currSetPropMthdBldr, setMethod);
            }
        }
    }

    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        public TableAttribute(string tableName)
        {
            Name = tableName;
        }

        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once UnusedAutoPropertyAccessor.Global
        public string Name { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class ColumnAttribute : Attribute
    {
        public ColumnAttribute(string name)
        {
            Name = name;
        }

        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once UnusedAutoPropertyAccessor.Global
        public string Name { get; set; }
    }

    // do not want to depend on data annotations that is not in client profile
    [AttributeUsage(AttributeTargets.Property)]
    public class KeyAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ExplicitKeyAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class WriteAttribute : Attribute
    {
        public WriteAttribute(bool write)
        {
            Write = write;
        }
        public bool Write { get; }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ComputedAttribute : Attribute
    {
    }
}

public partial interface ISqlAdapter
{
    bool SequenceSupported { get; }
    int Insert(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, ColumNameMapperDelegate columnNameMapper, object entityToInsert);

    //new methods for issue #336
    void AppendColumnName(StringBuilder sb, string columnName);
    void AppendColumnNameEqualsValue(StringBuilder sb, string columnName);
    void AppendParameter(StringBuilder sb, string columnName);
    void AppendSequenceNextValue(StringBuilder sb, string sequenceName);
}

public partial class SqlServerAdapter : ISqlAdapter
{

    public bool SequenceSupported => true;

    public int Insert(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, ColumNameMapperDelegate columnNameMapper, object entityToInsert)
    {
        var cmd = $"INSERT INTO {tableName} ({columnList}) VALUES ({parameterList});SELECT SCOPE_IDENTITY() [id]";
        var multi = connection.QueryMultiple(cmd, entityToInsert, transaction, commandTimeout);

        var first = multi.Read().FirstOrDefault();
        if (first == null || first?.id == null) return 0;

        // ReSharper disable once PossibleNullReferenceException
        var id = (int)first.id;
        var propertyInfos = keyProperties as PropertyInfo[] ?? keyProperties.ToArray();
        if (!propertyInfos.Any()) return id;

        var idProperty = propertyInfos.First();
        idProperty.SetValue(entityToInsert, Convert.ChangeType(id, idProperty.PropertyType), null);

        return id;
    }

    public void AppendColumnName(StringBuilder sb, string columnName)
    {
        sb.Append($"[{columnName}]");
    }

    public void AppendColumnNameEqualsValue(StringBuilder sb, string columnName)
    {
        sb.Append($"[{columnName}] = @{columnName}");
    }

    public void AppendParameter(StringBuilder sb, string parameterName)
    {
        sb.Append($"@{parameterName}");
    }

    public void AppendSequenceNextValue(StringBuilder sb, string sequenceName)
    {
        // https://msdn.microsoft.com/en-us/library/ff878370.aspx
        sb.Append("NEXT VALUE FOR ");
        sb.Append(sequenceName);
    }
}

public partial class SqlCeServerAdapter : ISqlAdapter
{
    public bool SequenceSupported => false;

    public int Insert(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, ColumNameMapperDelegate columnNameMapper, object entityToInsert)
    {
        var cmd = $"insert into {tableName} ({columnList}) values ({parameterList})";
        connection.Execute(cmd, entityToInsert, transaction, commandTimeout);
        var r = connection.Query("select @@IDENTITY id", transaction: transaction, commandTimeout: commandTimeout).ToList();

        if (r.First().id == null) return 0;
        var id = (int)r.First().id;

        var propertyInfos = keyProperties as PropertyInfo[] ?? keyProperties.ToArray();
        if (!propertyInfos.Any()) return id;

        var idProperty = propertyInfos.First();
        idProperty.SetValue(entityToInsert, Convert.ChangeType(id, idProperty.PropertyType), null);

        return id;
    }

    public void AppendColumnName(StringBuilder sb, string columnName)
    {
        sb.Append($"[{columnName}]");
    }

    public void AppendColumnNameEqualsValue(StringBuilder sb, string columnName)
    {
        AppendColumnName(sb, columnName);
        sb.Append(" = ");
        AppendParameter(sb, columnName);
    }

    public void AppendParameter(StringBuilder sb, string parameterName)
    {
        sb.Append($"@{parameterName}");
    }

    public void AppendSequenceNextValue(StringBuilder sb, string sequenceName)
    {
        throw new NotSupportedException("SQL CE does not support sequences");
    }
}

public partial class MySqlAdapter : ISqlAdapter
{

    public bool SequenceSupported => false;

    public int Insert(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, ColumNameMapperDelegate columnNameMapper, object entityToInsert)
    {
        var cmd = $"insert into {tableName} ({columnList}) values ({parameterList})";
        connection.Execute(cmd, entityToInsert, transaction, commandTimeout);
        var r = connection.Query("Select LAST_INSERT_ID() id", transaction: transaction, commandTimeout: commandTimeout);

        var id = r.First().id;
        if (id == null) return 0;
        var propertyInfos = keyProperties as PropertyInfo[] ?? keyProperties.ToArray();
        if (!propertyInfos.Any()) return Convert.ToInt32(id);

        var idp = propertyInfos.First();
        idp.SetValue(entityToInsert, Convert.ChangeType(id, idp.PropertyType), null);

        return Convert.ToInt32(id);
    }

    public void AppendColumnName(StringBuilder sb, string columnName)
    {
        sb.Append($"`{columnName}`");
    }

    public void AppendColumnNameEqualsValue(StringBuilder sb, string columnName)
    {
        AppendColumnName(sb, columnName);
        sb.Append(" = ");
        AppendParameter(sb, columnName);
    }

    public void AppendParameter(StringBuilder sb, string parameterName)
    {
        sb.Append($"@{parameterName}");
    }

    public void AppendSequenceNextValue(StringBuilder sb, string sequenceName)
    {
        throw new NotSupportedException("MySql does not support sequences");
    }
}


public partial class PostgresAdapter : ISqlAdapter
{

    public bool SequenceSupported => true;

    public int Insert(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, ColumNameMapperDelegate columnNameMapper, object entityToInsert)
    {
        var sb = new StringBuilder();
        sb.Append($"insert into {tableName} ({columnList}) values ({parameterList})");

        // If no primary key then safe to assume a join table with not too much data to return
        var propertyInfos = keyProperties as PropertyInfo[] ?? keyProperties.ToArray();
        if (!propertyInfos.Any())
            sb.Append(" RETURNING *");
        else
        {
            sb.Append(" RETURNING ");
            var first = true;
            foreach (var property in propertyInfos)
            {
                if (!first)
                    sb.Append(", ");
                first = false;
                sb.Append($"\"{columnNameMapper(property)}\"");
            }
        }

        var results = connection.Query(sb.ToString(), entityToInsert, transaction, commandTimeout: commandTimeout).ToList();

        // Return the key by assigning the corresponding property in the object - by product is that it supports compound primary keys
        var id = 0;
        foreach (var p in propertyInfos)
        {
            var value = ((IDictionary<string, object>)results.First())[columnNameMapper(p)];
            p.SetValue(entityToInsert, value, null);
            if (id == 0)
                id = Convert.ToInt32(value);
        }
        return id;
    }

    public void AppendColumnName(StringBuilder sb, string columnName)
    {
        sb.Append($"\"{columnName}\"");
    }

    public void AppendColumnNameEqualsValue(StringBuilder sb, string columnName)
    {
        AppendColumnName(sb, columnName);
        sb.Append(" = ");
        AppendParameter(sb, columnName);
    }

    public void AppendParameter(StringBuilder sb, string parameterName)
    {
        sb.Append($"@{parameterName}");
    }

    public void AppendSequenceNextValue(StringBuilder sb, string sequenceName)
    {
        // http://www.postgresql.org/docs/current/static/sql-createsequence.html
        sb.Append($"nextval('{sequenceName}')");
    }
}

public partial class SQLiteAdapter : ISqlAdapter
{
    public bool SequenceSupported => false;

    public int Insert(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName,
        string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties,
        ColumNameMapperDelegate columnNameMapper, object entityToInsert)
    {
        var cmd = $"INSERT INTO {tableName} ({columnList}) VALUES ({parameterList}); SELECT last_insert_rowid() id";
        var multi = connection.QueryMultiple(cmd, entityToInsert, transaction, commandTimeout);

        var id = (int)multi.Read().First().id;
        var propertyInfos = keyProperties as PropertyInfo[] ?? keyProperties.ToArray();
        if (!propertyInfos.Any()) return id;

        var idProperty = propertyInfos.First();
        idProperty.SetValue(entityToInsert, Convert.ChangeType(id, idProperty.PropertyType), null);

        return id;
    }

    public void AppendColumnName(StringBuilder sb, string columnName)
    {
        sb.Append($"\"{columnName}\"");
    }

    public void AppendColumnNameEqualsValue(StringBuilder sb, string columnName)
    {
        sb.Append($"\"{columnName}\" = @{columnName}");
    }

    public void AppendParameter(StringBuilder sb, string parameterName)
    {
        sb.Append($"@{parameterName}");
    }

    public void AppendSequenceNextValue(StringBuilder sb, string sequenceName)
    {
        throw new NotSupportedException("MySql does not support sequences");
    }
}

#if !COREFX
/// <summary>
/// SIM: TODO: this does not work
///  This is hacked up from the Postgresql adapter and currently untested - 
///  in any case, probably YAGNI for now.
/// </summary>
public partial class OracleAdapter : ISqlAdapter
{

    public bool SequenceSupported => true;

    public int Insert(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName,
        string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, ColumNameMapperDelegate columnNameMapper,
        object entityToInsert)
    {
        var parameters = new OracleDynamicParameters(entityToInsert);
        string command = CreateInsertSql(tableName, columnList, parameterList, keyProperties, parameters, columnNameMapper);

        connection.Execute(command, parameters, transaction, commandTimeout: commandTimeout);

        // Return the key by assigning the corresponding property in the object - by product is that it supports compound primary keys
        var id = 0;
        foreach (var property in keyProperties)
        {
            int value = parameters.Get<int>($"newid_{property.Name}");
            property.SetValue(entityToInsert, value, null);
            if (id == 0) { id = value; }
        }
        return id;
    }

    /// <summary>
    /// SIM Refactor this into a REF CURSOR returning anonymous block?
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="columnList"></param>
    /// <param name="parameterList"></param>
    /// <param name="keyProperties"></param>
    /// <param name="parameters"></param>
    /// <returns></returns>
    protected string CreateInsertSql(string tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, OracleDynamicParameters parameters, ColumNameMapperDelegate columNameMapper)
    {
        var sb = new StringBuilder();
        sb.Append($"insert into {tableName} ({columnList}) values ({parameterList})");

        // If no primary key then safe to assume a join table with not too much data to return
        if (!keyProperties.Any())
            sb.Append(" RETURNING *");
        else
        {
            sb.Append(" RETURNING ");
            var first = true;
            foreach (var property in keyProperties)
            {
                if (!first)
                    sb.Append(", ");
                first = false;
                //sb.Append(property.Name);
                sb.Append(columNameMapper(property));
                sb.Append(" INTO ");

                string parameterName = $"newid_{property.Name}";

                AppendParameter(sb, parameterName);
                parameters.Add(parameterName, dbType: OracleDbType.Int64, direction: ParameterDirection.ReturnValue);
            }
        }
        return sb.ToString();
    }

    public void AppendColumnName(StringBuilder sb, string columnName)
    {
        // In Oracle when column names are quoted they are case sensitive. Because 
        // of this its more user friendly to not quote column names.
        sb.Append(columnName);
    }

    public void AppendColumnNameEqualsValue(StringBuilder sb, string columnName)
    {
        AppendColumnName(sb, columnName);
        sb.Append(" = ");
        AppendParameter(sb, columnName);
    }


    public void AppendParameter(StringBuilder sb, string parameterName)
    {
        sb.Append($":{parameterName}");
    }

    public void AppendSequenceNextValue(StringBuilder sb, string sequenceName)
    {
        sb.Append(sequenceName);
        sb.Append(".NEXTVAL");
    }
}
#endif