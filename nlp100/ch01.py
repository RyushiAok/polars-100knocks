import numpy as np


def p_000() -> None:
    stressed = "stressed"
    print(stressed[::-1])


def p_001() -> None:
    s = "パタトクカシーー"
    print(s[::2])


def p_002() -> None:
    s1 = "パトカー"
    s2 = "タクシー"
    ans = "".join(a + b for a, b in zip(s1, s2))
    print(ans)


def p_003() -> None:
    s = "Now I need a drink, alcoholic of course, after the heavy lectures involving quantum mechanics."  # noqa: E501
    ans = list(map(len, s.split()))
    print(ans)


def p_004() -> None:
    s = "Hi He Lied Because Boron Could Not Oxidize Fluorine. New Nations Might Also Sign Peace Security Clause. Arthur King Can."  # noqa: E501
    words = s.split()
    indexes = set(map(lambda x: x - 1, [1, 5, 6, 7, 8, 9, 15, 16, 19]))

    abbreviation_map = {
        word[:1] if i in indexes else word[:2]: i + 1
        for i, word in enumerate(words)
    }
    print(abbreviation_map)


def n_gram(words: list[str], n: int) -> list[list[str]]:
    return [words[i : i + n] for i in range(len(words) - n + 1)]  # noqa: E203


def p_005() -> None:
    s = "I am an NLPer"
    words_bi_gram = n_gram(s.split(), 2)
    chars_bi_gram = n_gram(list(s), 2)
    print("単語bi-gram:", words_bi_gram)
    print("文字bi-gram:", chars_bi_gram)


def p_006() -> None:
    x = {"".join(char) for char in n_gram(list("paraparaparadise"), 2)}
    y = {"".join(char) for char in n_gram(list("paragraph"), 2)}
    print("x:", x)
    print("y:", y)
    print("和集合:", x | y)
    print("積集合:", x & y)
    print("差集合:", x - y)
    print("seがxに含まれるか:", {"se"} <= x)
    print("seがyに含まれるか:", {"se"} <= y)


def p_007() -> None:
    x = 12
    y = "気温"
    z = 22.4
    print(f"{x}時の{y}は{z}")


def p_008() -> None:
    def cipher(s: str) -> str:
        return "".join(
            chr(219 - ord(char)) if char.islower() else char for char in s
        )

    message = "the quick brown fox jumps over the lazy dog"
    ans = cipher(message)
    print(ans)
    ans = cipher(ans)
    print(ans)


def p_009() -> None:
    def typoglycemia(s: str) -> str:
        words = s.split()
        return " ".join(
            word[0]
            + "".join(np.random.permutation(list(word[1:-1])))
            + word[-1]
            if len(word) > 4
            else word
            for word in words
        )

    message = "I couldn’t believe that I could actually understand what I was reading : the phenomenal power of the human mind ."  # noqa: E501
    ans = typoglycemia(message)
    print(ans)
