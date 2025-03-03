import 'package:flagsmith/flagsmith.dart';
import 'package:test/test.dart';

import '../shared.dart';

void main() {
  group('[Streams]', () {
    late FlagsmithClient fs;
    setUp(() async {
      fs = await setupClientAdapter(StorageType.inMemory, caches: true);
      setupAdapter(fs, cb: (config, adapter) {});
    });
    tearDown(() {
      fs.close();
    });

    test('Loading state changing during reloading of flags', () async {
      expect(
          fs.loading,
          emitsInOrder(<FlagsmithLoading>[
            FlagsmithLoading.loading,
            FlagsmithLoading.loaded
          ]));
      await fs.getFeatureFlags(reload: true);
    });

    test('Stream successfuly changed when flag was updated', () async {
      await fs.reset();
      expect(await fs.isFeatureFlagEnabled(myFeatureName), true);
      expect(
          fs.stream(myFeatureName),
          emitsInOrder([
            TypeMatcher<Flag>()
                .having((s) => s.enabled, '$myFeatureName is enabled', true),
            TypeMatcher<Flag>().having(
                (s) => s.enabled, '$myFeatureName is not enabled', false),
          ]));
      await fs.testToggle(myFeatureName);
    });

    test('Subject value changed when flag was changed.', () async {
      await fs.reset();
      expect(await fs.isFeatureFlagEnabled(myFeatureName), true);
      expect(fs.subject(myFeatureName)?.stream.valueOrNull?.enabled, true);

      expect(
          fs.subject(myFeatureName)?.stream,
          emitsInOrder([
            TypeMatcher<Flag>()
                .having((s) => s.enabled, '$myFeatureName is enabled', true)
                .having((s) => s.feature.name, 'feature name is $myFeatureName',
                    myFeatureName),
            TypeMatcher<Flag>()
                .having(
                    (s) => s.enabled, '$myFeatureName is not enabled', false)
                .having((s) => s.feature.name, 'feature name is $myFeatureName',
                    myFeatureName),
          ]));
      fs.subject(myFeatureName)?.add(Flag.named(
          feature: Feature.named(name: myFeatureName), enabled: false));
    });
  });
}
