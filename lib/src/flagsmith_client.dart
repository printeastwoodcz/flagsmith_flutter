import 'dart:async';
import 'dart:convert';
import 'package:collection/collection.dart' show IterableExtension;
import 'package:flutter_client_sse/constants/sse_request_type_enum.dart';
import 'package:flutter_client_sse/flutter_client_sse.dart';
import 'package:rxdart/subjects.dart';
import 'package:dio/dio.dart';

import '../flagsmith.dart';

/// Flagsmith client initialization
///
/// [config] configuration for http client and endpoints
/// [apiKey] api key for your enviornment
/// [seeds] default values before update from Flagsmith env.

class FlagsmithClient {
  static final String authHeader = 'X-Environment-Key';
  static final String acceptHeader = 'Accept';
  static final String userAgentHeader = 'User-Agent';
  static final String applicationJson = 'application/json';
  static final String textEventStream = 'text/event-stream';
  static final String environmentUpdatedEvent = 'environment_updated';
  static final String updatedAtKey = 'updated_at';

  void log(String message) {
    if (flagsmithDebug) {
      // ignore: avoid_print
      print('Flagsmith: $message');
    }
  }

  final String apiKey;
  final FlagsmithConfig config;
  late StorageProvider storageProvider;
  CoreStorage? storage;
  late Dio _api;
  final Set<Flag> _flags = {};
  final List<Flag> seeds;

  Set<Flag> get cachedFlags => _flags;

  //A map of flag names to the amount of times they have been evaluated in the last 10 seconds
  final Map<String, int> flagAnalytics = {};
  Timer? _analyticsTimer;

  final StreamController<FlagsmithLoading> _loading =
      StreamController.broadcast();

  Dio get client => _api;
  bool flagsmithDebug = false;

  late Stream<SSEModel> _sseStream;
  double? lastGetFlags;
  Identity? cachedUser;

  FlagsmithClient(
      {this.config = const FlagsmithConfig(),
      required this.apiKey,
      this.seeds = const <Flag>[],
      this.storage}) {
    flagsmithDebug = config.isDebug;
    _api = _apiClient();
    storageProvider = prepareStorage(storage: storage, config: config);
    if (config.enableAnalytics) {
      _setupAnalyticsTimer(config.analyticsInterval);
    }
    if (config.enableRealtimeUpdates) {
      _setupRealtimeUpdates(config.realtimeUpdatesBaseURI);
    }
  }

  Future<void> _setupAnalyticsTimer(int analyticsInterval) async {
    _analyticsTimer?.cancel();
    _analyticsTimer = Timer.periodic(
        Duration(milliseconds: analyticsInterval), (_) => syncAnalyticsData());
  }

  Future<Response<dynamic>?> syncAnalyticsData() async {
    try {
      final valGreaterThan0 =
          flagAnalytics.values.firstWhereOrNull((element) => element > 0);

      if (valGreaterThan0 != null) {
        final res = await _api.post<dynamic>(config.analyticsURI,
            data: Map<String, dynamic>.from(flagAnalytics));

        if ([200, 201, 202].contains(res.statusCode)) {
          flagAnalytics.clear();
        }
        return res;
      }
    } on DioException catch (e) {
      log('_setupAnalyticsTimer dioError: ${e.error}');
      throw FlagsmithApiException(e);
    } catch (e) {
      log('Exception: _setupAnalyticsTimer $e');
      throw FlagsmithException(e);
    }

    return null;
  }

  Future<void> _setupRealtimeUpdates(String realtimeUpdatesBaseUrl) async {
    final sseUrl = '$realtimeUpdatesBaseUrl$apiKey/stream';

    _sseStream = SSEClient.subscribeToSSE(
      method: SSERequestType.GET,
      url: sseUrl,
      header: {
        acceptHeader: '$applicationJson , $textEventStream',
      },
    );

    _sseStream.listen(
      (event) async {
        final shouldGetFlags = await onEventReceived(
          event,
          lastGetFlags,
        );

        if (shouldGetFlags) {
          await getFeatureFlags(user: cachedUser);
        }
      },
    );

    Future.delayed(
      Duration(milliseconds: config.reconnectToSSEInterval),
      () async {
        _setupRealtimeUpdates(config.realtimeUpdatesBaseURI);
      },
    );
  }

  Future<bool> onEventReceived(
    SSEModel event,
    double? lastGetFlags,
  ) async {
    if (event.event != environmentUpdatedEvent) {
      return false;
    }

    double? lastEventUpdate;

    try {
      final eventData = jsonDecode(event.data ?? '{}') as Map<String, dynamic>;
      final updatedAt = eventData[updatedAtKey];
      if (updatedAt != null && updatedAt is double) {
        lastEventUpdate = updatedAt;
      }
    } catch (e) {
      log('Error parsing JSON: $e');
    }

    //If the last event update is newer than the last time we got flags,
    // then we should update the flags
    return (lastEventUpdate ?? 0) > (lastGetFlags ?? 0);
  }

  static StorageProvider prepareStorage(
      {CoreStorage? storage, required FlagsmithConfig config}) {
    late CoreStorage store;
    if (storage != null) {
      store = storage;
    } else {
      switch (config.storageType) {
        case StorageType.custom:
          if (storage == null) {
            throw FlagsmithConfigException(Exception('When using StorageType.custom, a storage implementation must be provided'));
          }
          store = storage;
          break;
        default:
          store = InMemoryStorage();
          break;
      }
    }
    return StorageProvider(store,
        password: config.password, logEnabled: config.isDebug);
  }

  /// Initialization throught custom init services
  ///
  ///
  Future<void> initialize() async {
    await initStore(seeds: seeds);
  }

  /// Async initialization
  ///
  /// Returns [FlagsmithClient] after seeds are ready
  static Future<FlagsmithClient> init({
    FlagsmithConfig config = const FlagsmithConfig(isDebug: true),
    required String apiKey,
    List<Flag> seeds = const <Flag>[],
    CoreStorage? storage,
  }) async {
    final client = FlagsmithClient(
      config: config,
      apiKey: apiKey,
      seeds: seeds,
      storage: storage,
    );
    await client.initStore(seeds: seeds);
    return client;
  }

  Future<bool> initStore(
      {List<Flag> seeds = const <Flag>[], bool clear = false}) async {
    if (clear) {
      await storageProvider.clear();
    }
    await storageProvider.seed(items: seeds);
    final items = await storageProvider.getAll();
    _updateCaches(list: items);
    return true;
  }

  /// Reset
  ///
  /// reseting storage and re-seed defalt values
  Future<bool> reset() async {
    await storageProvider.clear();
    await storageProvider.seed(items: seeds);
    _updateCaches(list: seeds);
    return true;
  }

  /// Simple implementation of Http Client
  Dio _apiClient() {
    var dio = Dio(config.clientOptions)
      ..options.headers[authHeader] = apiKey
      ..options.headers[acceptHeader] = applicationJson
      ..options.followRedirects = true;

    if (config.isDebug) {
      dio.interceptors.add(LogInterceptor(
        requestHeader: true,
        requestBody: true,
        responseBody: true,
      ));
    }
    return dio;
  }

  /// Get a list of existing Features for the given environment and user
  ///
  /// [user] a user in context
  /// [traits] a list of user traits
  /// [reload] force reload from API
  /// Returns a list of feature flags
  Future<List<Flag>> getFeatureFlags(
      {Identity? user, List<Trait>? traits, bool reload = true}) async {
    if (!reload) {
      var result = await storageProvider.getAll();
      if (result.isNotEmpty) {
        result.sort((a, b) => a.feature.name.compareTo(b.feature.name));
      }
      _updateCaches(list: result);
      return result;
    }
    _loading.add(FlagsmithLoading.loading);
    final list =
        user == null ? await _getFlags() : await _getUserFlags(user, traits);
    _loading.add(FlagsmithLoading.loaded);
    return list;
  }

  /// Check if Feature flag exist and is enabled
  ///
  /// [featureName] an identifier for the feature
  /// [user] an identifier for the user
  /// [reload] force reload from API
  /// Returns true if feature flag exist and enabled, false otherwise
  Future<bool> hasFeatureFlag(String featureName,
      {Identity? user, bool? reload}) async {
    var flag = await _getFlagByName(featureName, user: user, reload: reload);
    return flag != null;
  }

  /// Check if Feature flag exist and is enabled in cache
  ///
  /// [featureName] an identifier for the feature
  /// [user] an identifier for the user
  /// Returns true if feature flag exist and enabled, false otherwise
  ///
  /// Returns only if [caches] are enabled in config exception otherwise
  ///
  bool hasCachedFeatureFlag(String featureName, {Identity? user}) {
    if (!config.caches) {
      log('Exception: caches are NOT enabled!');
      throw FlagsmithConfigException(Exception('caches are NOT enabled!'));
    }

    var feature = _flags.firstWhereOrNull((element) =>
        element.feature.name == featureName && element.enabled == true);
    return feature != null;
  }

  /// Check if Feature flag exist and is enabled
  ///
  /// [featureName] an identifier for the feature
  /// [user] an identifier for the user
  /// [reload] force reload from API
  /// Returns true if feature flag exist and enabled, null otherwise
  Future<bool> isFeatureFlagEnabled(String featureName,
      {Identity? user, bool? reload}) async {
    var flag = await _getFlagByName(featureName, user: user, reload: reload);
    return flag?.enabled ?? false;
  }

  /// Get feature flag value by [featureId] and optionally for a [user]
  ///
  /// [reload] force reload from API
  /// Returns String value of Feature Flag
  Future<String?> getFeatureFlagValue(String featureId,
      {Identity? user, bool? reload}) async {
    var flag = await _getFlagByName(featureId, user: user, reload: reload);
    return flag?.stateValue;
  }

  Future<Flag?> _getFlagByName(String featureName,
      {Identity? user, bool? reload}) async {
    cachedUser = user;
    var flags = await getFeatureFlags(user: user, reload: reload ?? false);
    var flag = flags
        .firstWhereOrNull((element) => element.feature.name == featureName);
    _incrementFlagAnalytics(flag);
    return flag;
  }

  /// Get cached feature flag value by [featureId]
  /// Returns String value of Feature Flag
  String? getCachedFeatureFlagValue(
    String featureId,
  ) {
    if (!config.caches) {
      log('Exception: caches are NOT enabled!');
      throw FlagsmithConfigException(Exception('caches are NOT enabled!'));
    }
    var feature = cachedFlags
        .firstWhereOrNull((element) => element.feature.name == featureId);
    _incrementFlagAnalytics(feature);
    return feature?.stateValue;
  }

  /// Internal function for collecting analytical data on flag usage
  void _incrementFlagAnalytics(Flag? flag) {
    if (flag != null && config.enableAnalytics) {
      if (flagAnalytics.containsKey(flag.key)) {
        flagAnalytics[flag.key] = flagAnalytics[flag.key]! + 1;
      } else {
        flagAnalytics[flag.key] = 1;
      }
    }
  }

  /// Remove feature flag from storage and caches
  Future<bool> removeFeatureFlag(String featureName) async {
    var result = await storageProvider.delete(featureName);
    if (config.caches) {
      _flags.removeWhere((element) => element.feature.name == featureName);
    }
    return result;
  }

  /// Get user trait for [user] with by [key]
  Future<Trait?> getTrait(Identity user, String key) async {
    var result = await _getUserTraits(user);
    return result.firstWhereOrNull((element) => element.key == key);
  }

  Future<List<Flag>> _getFlags() async {
    try {
      var response = await _api.get<List<dynamic>>(config.flagsURI);
      if (response.statusCode == 200) {
        lastGetFlags = DateTime.now().secondsSinceEpoch;

        var list = response.data!
            .map<Flag>((dynamic e) => Flag.fromJson(e as Map<String, dynamic>))
            .toList();

        await storageProvider.saveAll(list);
        final saved = await storageProvider.getAll()
          ..sort((a, b) => a.feature.name.compareTo(b.feature.name));
        _updateCaches(list: saved);

        return list;
      }
      return [];
    } on DioException catch (e) {
      log('_getFlags dioError: ${e.error}');
      throw FlagsmithApiException(e);
    } catch (e) {
      log('Exception: _getFlags $e');
      throw FlagsmithException(e);
    }
  }

  // Internal list of [user] flags
  Future<List<Flag>> _getUserFlags(Identity user, List<Trait>? traits) async {
    try {
      cachedUser = user;
      var identityData = user.toJson();
      if (traits != null && traits.isNotEmpty) {
        identityData['traits'] = traits.map((t) => t.toJson()).toList();
      }

      var response = await _api.post(config.identitiesURI, data: identityData);

      if (response.statusCode == 200) {
        lastGetFlags = DateTime.now().secondsSinceEpoch;

        if (response.data == null) {
          return [];
        }

        var data = FlagsAndTraits.fromJson(response.data!).flags ?? [];
        if (data.isNotEmpty) {
          data.sort((a, b) => a.feature.name.compareTo(b.feature.name));
        }

        await storageProvider.saveAll(data);
        final saved = await storageProvider.getAll()
          ..sort((a, b) => a.feature.name.compareTo(b.feature.name));
        _updateCaches(list: saved);

        return data;
      }
      return [];
    } on DioException catch (e) {
      log('_getFlags dioError: ${e.error}');
      throw FlagsmithApiException(e);
    } catch (e) {
      log('Exception: _getFlags $e');
      throw FlagsmithException(e);
    }
  }

  /// TRAITS
  ///
  /// Get all `user` traits with `keys`
  Future<List<Trait>> getTraits(Identity user, {List<String>? keys}) async {
    var result = await _getUserTraits(user);
    if (keys == null) {
      return result;
    }
    return result.where((element) => keys.contains(element.key)).toList();
  }

  /// Internal list of `user` traits
  Future<List<Trait>> _getUserTraits(Identity user) async {
    try {
      cachedUser = user;
      var params = {'identifier': user.identifier};
      if (user.transient ?? false) {
        params['transient'] = 'true';
      }
      var response = await _api.get<Map<String, dynamic>?>(config.identitiesURI,
          queryParameters: params);

      if (response.statusCode == 200) {
        if (response.data == null) {
          return [];
        }
        return FlagsAndTraits.fromJson(response.data!).traits ?? [];
      }
      return [];
    } on DioException catch (e) {
      log('_getFlags dioError: ${e.error}');
      throw FlagsmithApiException(e);
    } catch (e) {
      log('Exception: _getFlags $e');
      throw FlagsmithException(e);
    }
  }

  // Update trait for `user` with new value `traits`
  Future<TraitWithIdentity?> createTrait(
      {required TraitWithIdentity value}) async {
    try {
      var response =
          await _api.post<dynamic>(config.traitsURI, data: value.toJson());
      if (response.data == null) {
        return null;
      }
      return TraitWithIdentity.fromJson(response.data as Map<String, dynamic>);
    } on DioException catch (e) {
      log('_getFlags dioError: ${e.error}');
      throw FlagsmithApiException(e);
    } catch (e) {
      log('Exception: _getFlags $e');
      throw FlagsmithException(e);
    }
  }

  /// Bulk update of traits for `user` with list of `value`
  Future<List<TraitWithIdentity>?> updateTraits(
      {required List<TraitWithIdentity> value}) async {
    try {
      if (value.isEmpty) {
        return null;
      }

      final identifier = value[0].identity.identifier;
      final traitList = value
          .map((e) => <String, dynamic>{
                'trait_key': e.key,
                'trait_value': e.value,
              })
          .toList();

      final data = <String, dynamic>{
        'identifier': identifier,
        'traits': traitList,
      };
      var response = await _api.post<dynamic>(
        config.identitiesURI,
        data: data,
      );
      if (response.data == null || response.data['traits'] == null) {
        return null;
      }
      final responseData = List<Map<String, dynamic>>.from(
          response.data['traits'] as List<dynamic>);
      return responseData
          .map((e) => TraitWithIdentity(
                identity: Identity(identifier: identifier),
                key: e['trait_key'] as String,
                value: e['trait_value'],
              ))
          .toList();
    } on DioException catch (e) {
      log('_getFlags dioError: ${e.error}');
      throw FlagsmithApiException(e);
    } catch (e) {
      log('Exception: _getFlags $e');
      throw FlagsmithException(e);
    }
  }

  /// Internal updadte caches from list of featurs
  void _updateCaches({List<Flag> list = const <Flag>[]}) {
    if (config.caches) {
      _flags
        ..clear()
        ..addAll(list.toSet());
    }
  }

  /// clear all data from storage
  Future<bool> clearStore() async {
    if (config.caches) {
      _flags.clear();
    }
    return storageProvider.clear();
  }

  /// stream for listener
  Stream<Flag>? stream(String key) => storageProvider.stream(key);

  /// basic stream
  BehaviorSubject<Flag>? subject(String key) => storageProvider.subject(key);

  // Loading from API
  Stream<FlagsmithLoading> get loading => _loading.stream;

  void close() {
    _analyticsTimer?.cancel();
    SSEClient.unsubscribeFromSSE();
    _loading.close();
  }

  /// test toggle feature
  ///
  Future<bool> testToggle(String featureName) async {
    final result = await storageProvider.togggleFeature(featureName);
    final value = await storageProvider.read(featureName);
    if (config.caches) {
      _flags.removeWhere((element) => element.feature.name == featureName);
      if (value != null) {
        _flags.add(value);
      }
    }
    return result;
  }
}
