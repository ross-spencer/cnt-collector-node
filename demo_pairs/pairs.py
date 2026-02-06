"""DEX Pairs configuration"""

# pylint: disable = C0302
# fmt: off

DEX_PAIRS = [
  {
    "name": "ADA-USDA",
    "token1_policy": "",
    "token1_name": "lovelace",
    "token1_decimals": 6,
    "token2_policy": "fe7c786ab321f41c654ef6c1af7b3250a613c24e4213e0425a7ae456",
    "token2_name": "55534441",
    "token2_decimals": 6,
    "sources": [
      {
        "source": "MinSwapV2",
        "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
        "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
        "security_token_name": "4d5350"
      },
      {
        "source": "SundaeSwapV3",
        "address": "addr1x8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz26rnxqnmtv3hdu2t6chcfhl2zzjh36a87nmd6dwsu3jenqsslnz7e",
        "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
        "security_token_name": "000de140c6ad22f3f52ddd285876d67a7357d7cf25ee10fe942f3547e371bcd6"
      },
      {
        "source": "WingRidersV2",
        "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqc5jq5npz5xdnmdzjh7ez6e4j5xst29eqgcnmzyf60zmadsq3q9h0",
        "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
        "security_token_name": "4c"
      },
      {
        "source": "VyFi",
        "address": "addr1z955fyznplf6hnuf4tgzwkpjqwe8x5yq7n5yrehp3xkypm9ksyd8pn7amnr48geat0yft0uezfunealzy4ghl0cayp4snfzxkj",
        "security_token_policy": "f7f9777979a2a96777823f149e6696954f43967fc56cfc7095a33f98",
        "security_token_name": ""
      }
    ]
  },
  {
    "name": "FACT-ADA",
    "token1_policy": "a3931691f5c4e65d01c429e473d0dd24c51afdb6daf88e632a6c1e51",
    "token1_name": "6f7263666178746f6b656e",
    "token1_decimals": 6,
    "token2_policy": "",
    "token2_name": "lovelace",
    "token2_decimals": 6,
    "sources": [
      {
        "source": "MinSwap",
        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
        "security_token_name": "b4ba2b47edce71234f328fa20efdb25c3f96e348ca19a683193880489bb368db"
      },
      {
        "source": "MinSwapV2",
        "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
        "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
        "security_token_name": "4d5350"
      },
      {
        "source": "SundaeSwapV3",
        "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
        "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
        "security_token_name": "000de140a5b624b96af21138b6dff057e0499e7f767fcfe7ac8adb549f3818d7"
      },
      {
        "source": "WingRiders",
        "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnayg6pp9snyy9v7uwarxd7dqc5k52egtc49y5w5h3nqqdy6qs2nzs8y",
        "security_token_policy": "026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570",
        "security_token_name": "4c"
      },
      {
        "source": "WingRidersV2",
        "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqadav244fusrvfjcrdra646ude8dlctzmv6wvsqtndwrdfsvg56qt",
        "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
        "security_token_name": "4c"
      },
      {
        "source": "SundaeSwap",
        "address": "addr1w9qzpelu9hn45pefc0xr4ac4kdxeswq7pndul2vuj59u8tqaxdznu",
        "security_token_policy": "0029cb7c88c7567b63d1a512c0ed626aa169688ec980730c0473b913",
        "security_token_name": "7020fb04"
      }
    ]
  },
  {
    "name": "HOSKY-ADA",
    "token1_policy": "a0028f350aaabe0545fdcb56b039bfb08e4bb4d8c4d7c3c7d481c235",
    "token1_name": "484f534b59",
    "token1_decimals": 0,
    "token2_policy": "",
    "token2_name": "lovelace",
    "token2_decimals": 6,
    "sources": [
      {
        "source": "MinSwap",
        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
        "security_token_name": "11e236a5a8826f3f8fbc1114df918b945b0b5d8f9c74bd383f96a0ea14bffade"
      },
      {
        "source": "MinSwapV2",
        "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
        "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
        "security_token_name": "4d5350"
      },
      {
        "source": "SundaeSwapV3",
        "address": "addr1x8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz26rnxqnmtv3hdu2t6chcfhl2zzjh36a87nmd6dwsu3jenqsslnz7e",
        "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
        "security_token_name": "000de140455422de9777d248aaaa71da9e17f67ddb6e003aadea1f4f97d24ddd"
      },
      {
        "source": "WingRiders",
        "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnayvgedqwg3tvxxvhlgnrzujmpw9qful70s6tfga5gyadds4qsksxeq",
        "security_token_policy": "026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570",
        "security_token_name": "4c"
      },
      {
        "source": "WingRidersV2",
        "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqahnnkdeex48glxptpj65zc5jyp7rhynd0fplm3sznpl7xsu9g7vd",
        "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
        "security_token_name": "4c"
      },
      {
        "source": "SundaeSwap",
        "address": "addr1w9qzpelu9hn45pefc0xr4ac4kdxeswq7pndul2vuj59u8tqaxdznu",
        "security_token_policy": "0029cb7c88c7567b63d1a512c0ed626aa169688ec980730c0473b913",
        "security_token_name": "702011"
      }
    ]
  },
  {
    "name": "IAG-ADA",
    "token1_policy": "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114",
    "token1_name": "494147",
    "token1_decimals": 6,
    "token2_policy": "",
    "token2_name": "lovelace",
    "token2_decimals": 6,
    "sources": [
      {
        "source": "MinSwap",
        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
        "security_token_name": "bdfd144032f09ad980b8d205fef0737c2232b4e90a5d34cc814d0ef687052400"
      },
      {
        "source": "MinSwapV2",
        "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
        "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
        "security_token_name": "4d5350"
      },
      {
        "source": "SundaeSwapV3",
        "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
        "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
        "security_token_name": "000de1406f79e3e55eef82b9d03cf62cc3d4a6d0d03b00bf7b1b43330f829779"
      },
      {
        "source": "WingRiders",
        "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnayw3e26v9tyaqqvhqlzngl5afw2ls5j5se2z7msh30pz0vwsscveaf",
        "security_token_policy": "026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570",
        "security_token_name": "4c"
      },
      {
        "source": "WingRidersV2",
        "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhq7lz3zuxz0l95ne0pwxdy0r7uvyqmx39l0nv4jyc9g59ngsj543je",
        "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
        "security_token_name": "4c"
      },
      {
        "source": "CSwap",
        "address": "addr1z8ke0c9p89rjfwmuh98jpt8ky74uy5mffjft3zlcld9h7ml3lmln3mwk0y3zsh3gs3dzqlwa9rjzrxawkwm4udw9axhs6fuu6e",
        "security_token_policy": "a00d48eff61d8cfd86b5795d0b15015b84a33f139f22e7c8e3005c34",
        "security_token_name": "63"
      },
      {
        "source": "VyFi",
        "address": "addr1z923yccpjgvf3lk2n3u4zl25vjm7227y5lswvyg2g6387z8qy02w7ayk0lyyrf080l5zusdpkg8se9x7fcke6vaz42lq7dg4ty",
        "security_token_policy": "91273656a81cc90ae6a5403a39052eeae71f17332cc1928be01ec656",
        "security_token_name": ""
      }
    ]
  },
  {
    "name": "NIGHT-ADA",
    "token1_policy": "0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1fa",
    "token1_name": "4e49474854",
    "token1_decimals": 6,
    "token2_policy": "",
    "token2_name": "lovelace",
    "token2_decimals": 6,
    "sources": [
      {
        "source": "MinSwapV2",
        "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
        "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
        "security_token_name": "4d5350"
      },
      {
        "source": "SundaeSwapV3",
        "address": "addr1x8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz26rnxqnmtv3hdu2t6chcfhl2zzjh36a87nmd6dwsu3jenqsslnz7e",
        "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
        "security_token_name": "000de1405b5d1f9da977498b5faf3efb83693b0442ed5f49d00d9b986a409c0b"
      },
      {
        "source": "WingRidersV2",
        "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqacq84zk0xfmaxftjjzm9f2q5mauka7jcrdjj89ehx3ex8qa2epvx",
        "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
        "security_token_name": "4c"
      }
    ]
  },
  {
    "name": "NVL-ADA",
    "token1_policy": "5b26e685cc5c9ad630bde3e3cd48c694436671f3d25df53777ca60ef",
    "token1_name": "4e564c",
    "token1_decimals": 6,
    "token2_policy": "",
    "token2_name": "lovelace",
    "token2_decimals": 6,
    "sources": [
      {
        "source": "MinSwap",
        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
        "security_token_name": "0da982986c489a86496cb6dd417891233bde153cdd73673e5d3aa6930d6ccdd6"
      },
      {
        "source": "MinSwapV2",
        "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
        "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
        "security_token_name": "4d5350"
      },
      {
        "source": "SundaeSwapV3",
        "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
        "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
        "security_token_name": "000de1405c2c941eee1b5952a144f16d0b072fd9a7fd829487d375c25ee236f7"
      }
    ]
  },
  {
    "name": "SNEK-ADA",
    "token1_policy": "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f",
    "token1_name": "534e454b",
    "token1_decimals": 0,
    "token2_policy": "",
    "token2_name": "lovelace",
    "token2_decimals": 6,
    "sources": [
      {
        "source": "MinSwap",
        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
        "security_token_name": "63f2cbfa5bf8b68828839a2575c8c70f14a32f50ebbfa7c654043269793be896"
      },
      {
        "source": "MinSwapV2",
        "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
        "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
        "security_token_name": "4d5350"
      },
      {
        "source": "SundaeSwapV3",
        "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
        "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
        "security_token_name": "000de140cacb7fd5f5b84bf876d40dc60d4991c72112d78d76132b1fb769e6ad"
      },
      {
        "source": "WingRiders",
        "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnay2lz4g5wy95jwh2l6ca2jyq5xu8aga0fh3jyplef6m0npeslcq0pj",
        "security_token_policy": "026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570",
        "security_token_name": "4c"
      },
      {
        "source": "WingRidersV2",
        "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqlhhdq34c6wgm2u5xkg84nqkql6vq6fzm5grzcequr2rmwqwgf0zz",
        "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
        "security_token_name": "4c"
      },
      {
        "source": "Splash",
        "address": "addr1x89ksjnfu7ys02tedvslc9g2wk90tu5qte0dt4dge60hdudj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrsg0g63z",
        "security_token_policy": "ce1a4f1103fca3f93c1ba9b4e87fb0d9e855d66965ca3cf45165824a",
        "security_token_name": "534e454b5f4144415f4e4654"
      },
      {
        "source": "CSwap",
        "address": "addr1z8ke0c9p89rjfwmuh98jpt8ky74uy5mffjft3zlcld9h7ml3lmln3mwk0y3zsh3gs3dzqlwa9rjzrxawkwm4udw9axhs6fuu6e",
        "security_token_policy": "8e50527b8cc1763348b393dca349bf04385ee12d4568afa0c8a457a9",
        "security_token_name": "63"
      },
      {
        "source": "VyFi",
        "address": "addr1zy3jcyykdnjd3enu96hp0w6hct85s499w5y6w5hmk0qzh50qy02w7ayk0lyyrf080l5zusdpkg8se9x7fcke6vaz42lqwjjflq",
        "security_token_policy": "96c31772282e6ae5c629120471c5bbcdef538226b31b97d74c50ca3c",
        "security_token_name": ""
      },
      {
        "source": "SundaeSwap",
        "address": "addr1w9qzpelu9hn45pefc0xr4ac4kdxeswq7pndul2vuj59u8tqaxdznu",
        "security_token_policy": "0029cb7c88c7567b63d1a512c0ed626aa169688ec980730c0473b913",
        "security_token_name": "70201f04"
      }
    ]
  },
  {
    "name": "XER-ADA",
    "token1_policy": "6d06570ddd778ec7c0cca09d381eca194e90c8cffa7582879735dbde",
    "token1_name": "584552",
    "token1_decimals": 6,
    "token2_policy": "",
    "token2_name": "lovelace",
    "token2_decimals": 6,
    "sources": [
      {
        "source": "MinSwapV2",
        "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
        "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
        "security_token_name": "4d5350"
      }
    ]
  }
]
