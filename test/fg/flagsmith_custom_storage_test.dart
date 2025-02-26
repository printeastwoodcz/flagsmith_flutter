import 'package:flagsmith/flagsmith.dart';
import 'package:test/test.dart';
import '../shared.dart';

class TestStorage extends InMemoryStorage {
  bool initCalled = false;

  @override
  Future<void> init() async {
    initCalled = true;
    return Future.value();
  }
}

void main() {
  group('FlagsmithClient with custom storage', () {
    test('Should initialize with custom storage', () async {
      final customStorage = TestStorage();

      // Initialize the client with custom storage
      final client = await FlagsmithClient.init(
        apiKey: apiKey,
        config: const FlagsmithConfig(
          storageType: StorageType.custom,
          isDebug: true,
        ),
        storage: customStorage,
        seeds: <Flag>[
          Flag.seed('feature', enabled: true),
        ],
      );

      // Verify the client was initialized successfully
      expect(client, isA<FlagsmithClient>());
      expect(customStorage.initCalled, isTrue);

      // Clean up
      client.close();
    });

    test('Should throw error when StorageType.custom is used without providing storage', () async {
      expect(
        () => FlagsmithClient.init(
          apiKey: apiKey,
          config: const FlagsmithConfig(
            storageType: StorageType.custom,
            isDebug: true,
          ),
          seeds: <Flag>[
            Flag.seed('feature', enabled: true),
          ],
        ),
        throwsA(isA<FlagsmithConfigException>()),
      );
    });

  });
}
